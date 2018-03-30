#!/usr/bin/env python
# coding: utf-8 
# created on 2018.03.27 using PyCharm 
# project impresso-image-acquisition

"""
Impresso project: Sanity check for images, original and canonical

Usage:
    check-image.py --command==<c> --newspapers=<np> --original-dir=<od> --canonical-dir=<cd>  --report-dir==<rd>  [--log-file=<lf> --verbose --parallelize]

Options:
    --command=<c>       Command to be executed, 'check_original' or 'check_canonical'
    --original-dir=<od>    Base directory containing one sub-directory for each journal.
    --canonical-dir=<cd>    Base directory containing one sub-directory for each journal.
    --newspapers=<np>   List of titles to be considered, as blank separated terms. "EXP GDL"
    --report-dir==<rd>  Directory where to write the report files.
    --log-file=<lf>      Log file; when missing print log to stdout
    --verbose           Verbose log messages (good for debugging).
    --parallelize       Parallelize the import.

"""

import docopt
import logging
import os
import zipfile
from collections import defaultdict, Counter
import glob
from dask import delayed
from enum import Enum
import time
import humanize

from impresso_commons.images import img_utils
import impresso_commons.path as path
import impresso_commons.utils as utils

logger = logging.getLogger(__name__)
original_counter = defaultdict(list)


__author__ = "maudehrmann"


class OriginalImageCase(Enum):
    """ Possible cases w.r.t. image format distribution in issues from Olive archives."""
    issues_wo_zip = 'issues_wo_zip'
    issues_with_corruptedzip = 'issues_with_corruptedzip'
    # issues fully covered with one img type
    issues_homogeneouscoverage_tifs = 'issues_homogeneouscoverage_tifs'
    issues_homogeneouscoverage_pngs = 'issues_homogeneouscoverage_pngs'
    issues_homogeneouscoverage_jpgs = 'issues_homogeneouscoverage_jpgs'
    issues_homogeneouscoverage_singlepngs = 'issues_homogeneouscoverage_singlepngs'
    # issues fully covered with mixed img type
    issues_heterocoverage_all = 'issues_heterocoverage_all'
    issues_heterocoverage_tif_png = 'issues_heterocoverage_tif_png'
    issues_heterocoverage_tif_jpg = 'issues_heterocoverage_tif_jpg'
    issues_heterocoverage_png_jpg = 'issues_heterocoverage_png_jpg'
    # missing img
    issues_missing_pageimg = 'issues_missing_pageimg'


class CanonicalImageCase(Enum):
    """ Possible cases w.r.t. canonical images (jp2) in impresso archives."""
    issues_wo_jp2 = 'issues w/o jp2'
    issues_with_wrongnumber_infofile = 'issues w/ incorrect # infofile'
    page_wo_jp2 = 'pages w/o jp2'
    issues_wo_infofile = 'issues w/o infofile'
    infofile_with_wrongnumber_img = 'infofile w/ incorrect # img'
    image_incorrect_filename = 'jp2 w/ incorrect filename'
    imagefile_wo_journalname = 'jp2 w/o journalname'
    infofile_wo_journalname = 'infofile w/o journalname'
    imagefile_wo_correctdate = 'jp2 w/ incorrect date'
    infofile_wo_correctdate = 'infofile w/ incorrect date'


class StatsImage(Enum):
    number_tif = "number tif"
    number_png = "number png"
    number_jpg = "number jpg"
    size_jp2 = "total size jp2"
    number_original_pagefolder = "total original page folders"
    number_canonical_jp2 = "total canonical jp2"


def initialize_case_dict():
    d = {}
    for name, member in CanonicalImageCase.__members__.items():
        d.setdefault(member.value, [])
    for name, member in StatsImage.__members__.items():
        d.setdefault(member.value, [])
    return d


def initialize_stats_dict():
    d = {}
    for name, member in StatsImage.__members__.items():
        d.setdefault(member.value, 0)
    return d


def check_image_pairs(page_folders, jp2):
    """
    Check whether each image of page_folders have a corresponding jp2 one.

    :param page_folders: list which contains full paths to page folder (.../GDL/1900/01/10/1)
    :type page_folders: array
    :param jp2: list which contains full paths to jp2 images (.../GDL/1900/01/10/a/GDL-1900-01-10-a-p0001.jp2)
    :type jp2: array
    :return: list of pages (paths) without a jp2 twin
    :rtype: array
    """
    d = {}
    res = []

    # put jp2 in hash table
    for img in jp2:
        i = img[-8:-4]
        d[i] = ""
    # check if original pages have a jp2
    for page in page_folders:
        p = page[-1:].zfill(4)
        if p not in d:
            res.append(page)
    return res


def check_canonical(issue_dir_original, issue_dir_canonical):
    """ Parses the impresso original and canonical image directories and detects anomalies.

    :param issue_dir_original: the original issue in the source folder
    :type issue_dir_original: IssueDir
    :param issue_dir_canonical: the canonical issue in the working folder
    :type issue_dir_canonical: IssueDir
    :return: a tuple consisting of: \n
        1) dictionary with k = CanonicalImageCase v = list of issues of that case and \n
        2) dictionary keeping the counts of source image formats (how many were generated from tif, jpg etc.)
    :rtype: tuple
    """

    # variables
    short_cano = path.get_issueshortpath(issue_dir_canonical)
    # local_counter = defaultdict(int)  # keep counts of image formats
    local_stats_dict = initialize_stats_dict()
    local_cases_dict = {}  # keep the paths (values) of problematic 'CanonicalImageCase' (key)

    # get original page folders (to compare with what is produced on the canonical side)
    working_archive = os.path.join(issue_dir_original.path, "Document.zip")
    if os.path.isfile(working_archive):
        try:
            archive = zipfile.ZipFile(working_archive)
        except zipfile.BadZipfile as e:
            logger.info(f"Bad zip file in {issue_dir_original.path}")

        page_folders = img_utils.get_page_folders(archive)
        # store number of original page folders
        local_stats_dict[StatsImage.number_original_pagefolder.value] += len(page_folders)

    # get canonical page material
    jp2 = glob.glob(os.path.join(issue_dir_canonical.path, "*.jp2"))
    info = glob.glob(os.path.join(issue_dir_canonical.path, "*.txt"))

    # jp2 simple check
    if not jp2:
        local_cases_dict.setdefault(CanonicalImageCase.issues_wo_jp2.value, []).append(short_cano)
    else:
        # store number of jp2
        local_stats_dict[StatsImage.number_canonical_jp2.value] += len(jp2)
        # check img names comply with naming convention
        jp2_bytes = 0
        for img_file in jp2:
            # get short path and basename of jp2
            shortjp2 = img_file[img_file.index(issue_dir_canonical.journal):]
            basename = os.path.splitext(os.path.basename(img_file))[0]
            # jp2 does not comply with naming convention
            if not path.check_filenaming(basename):
                local_cases_dict.setdefault(CanonicalImageCase.image_incorrect_filename.value, []).append(shortjp2)
            # jp2 does not contain the journal acronym
            if issue_dir_original.journal not in basename:
                local_cases_dict.setdefault(CanonicalImageCase.imagefile_wo_journalname.value, []).append(shortjp2)
            # jp2 does not contain the issue date
            if str(issue_dir_original.date) not in basename:
                local_cases_dict.setdefault(CanonicalImageCase.imagefile_wo_correctdate.value, []).append(shortjp2)
            # get size of jp2
            jp2_bytes += os.path.getsize(img_file)
            local_stats_dict[StatsImage.size_jp2.value] += jp2_bytes

    # info simple check
    if not info:
        local_cases_dict.setdefault(CanonicalImageCase.issues_wo_infofile.value, []).append(short_cano)
    else:
        # short path of info file
        shortinfo = info[0][info[0].index(issue_dir_canonical.journal):]

        # check number of info file
        if len(info) != 1:
            local_cases_dict.setdefault(CanonicalImageCase.issues_with_wrongnumber_infofile.value, []).append(short_cano)

        # get basename of info file
        baseinfo = os.path.basename(info[0])
        infojournal = baseinfo[:baseinfo.index("-")]
        # info file does not contain journal name
        if issue_dir_original.journal != infojournal:
            local_cases_dict.setdefault(CanonicalImageCase.infofile_wo_journalname.value, []).append(shortinfo)
        # info file does not contain the issue date
        if str(issue_dir_original.date) not in baseinfo:
            local_cases_dict.setdefault(CanonicalImageCase.infofile_wo_correctdate.value, []).append(shortinfo)

    # info and jp2 detailed check
    if info and jp2:
        # check if all original pages have a corresponding canonical image
        list_pages_wo_jp2 = check_image_pairs(page_folders, jp2)
        if list_pages_wo_jp2:
            for page in list_pages_wo_jp2:
                shortpage = page[page.index(issue_dir_canonical.journal):]
                local_cases_dict.setdefault(CanonicalImageCase.page_wo_jp2.value, []).append(shortpage)

        # check if same number of img reported in info file than in reality
        nb_lines = sum(1 for line in open(info[0]))
        if nb_lines != len(jp2):
            local_cases_dict.setdefault(CanonicalImageCase.infofile_with_wrongnumber_img.value, []).append(shortinfo)

        # keep stats of source image format
        for line in open(info[0]):
            if "tif" in line:
                # local_counter['tif'] += 1
                local_stats_dict[StatsImage.number_tif.value] += 1
            elif "png" in line:
                local_stats_dict[StatsImage.number_png.value] += 1
                # local_counter['png'] += 1
            elif "jpg" in line:
                local_stats_dict[StatsImage.number_jpg.value] += 1
                # local_counter['jpg'] += 1

    return local_cases_dict, local_stats_dict


def check_original(issue_dir):
    """Parse Olive images of a journal issue.

    :param issue_dir    : the journal issue issue directory
    :type issue_dir     : IssueDir
    """
    # get zip archive and check it
    working_archive = os.path.join(issue_dir.path, "Document.zip")

    if not os.path.isfile(working_archive):
        return issue_dir.path, OriginalImageCase.issues_wo_zip
    else:
        try:
            archive = zipfile.ZipFile(working_archive)
        except zipfile.BadZipfile as e:
            return issue_dir.path, OriginalImageCase.issues_with_corruptedzip

        # if archive ok, proceed:
        # collect top dirs
        page_folders = img_utils.get_page_folders(archive)
        page_number = len(page_folders)

        # collect images
        tifs = img_utils.get_img_from_archive(archive, "Res/PageImg", ".tif")
        pngs = img_utils.get_img_from_archive(archive, "/Img", ".png", "/Pg")
        jpgs = img_utils.get_img_from_archive(archive, "/Img", ".jpg", "/Pg")

        # init counters
        pages_with_tifs = 0
        pages_with_pngs = 0
        pages_with_several_pngs = 0
        pages_with_one_pngs = 0
        pages_with_jpgs = 0

        for page in page_folders:
            page_digit = os.path.split(page)[1]

            # case 1: look for tif page
            tif = img_utils.get_tif(tifs, page_digit)
            if tif is not None:
                pages_with_tifs += 1

            # case 2: if not tif, look for png
            else:
                # check if png
                png = img_utils.get_png(pngs, page_digit)
                if png is not None:
                    pages_with_pngs += 1
                    # check when there is one or several pngs
                    # group by pages (there can be several images per pages):
                    d = defaultdict(list)
                    for i in pngs:
                        elems = i.split("/", 1)
                        d[elems[0]].append(elems[1])

                    for p, img_path in d.items():
                        if len(img_path) > 1:
                            pages_with_several_pngs += 1
                        elif len(img_path) == 1:
                            pages_with_one_pngs += 1

                # case 3: if no png, look for jpg
                else:
                    # check if jpg
                    jpg = img_utils.get_jpg(jpgs, page_digit)
                    if jpg is not None:
                        pages_with_jpgs += 1

        # reporting cases
        total = pages_with_tifs + pages_with_pngs + pages_with_jpgs
        total_pngs = pages_with_one_pngs + pages_with_several_pngs
        case = None
        if total == page_number:
            # mixed cases
            if pages_with_tifs != 0 and pages_with_pngs != 0 and pages_with_jpgs != 0:
                case = OriginalImageCase.issues_heterocoverage_all
            elif pages_with_tifs != 0 and pages_with_pngs != 0 and pages_with_jpgs == 0:
                case = OriginalImageCase.issues_heterocoverage_tif_png
            elif pages_with_tifs != 0 and pages_with_pngs == 0 and pages_with_jpgs != 0:
                case = OriginalImageCase.issues_heterocoverage_tif_jpg
            elif pages_with_tifs == 0 and pages_with_pngs != 0 and pages_with_jpgs != 0:
                case = OriginalImageCase.issues_heterocoverage_png_jpg
            # non mixed cases
            elif pages_with_tifs == total and pages_with_pngs == 0 and pages_with_jpgs == 0:
                case = OriginalImageCase.issues_homogeneouscoverage_tifs
            elif pages_with_tifs == 0 and pages_with_pngs == total and pages_with_jpgs == 0:
                case = OriginalImageCase.issues_homogeneouscoverage_pngs
            elif pages_with_tifs == 0 and pages_with_pngs == 0 and pages_with_jpgs == total:
                case = OriginalImageCase.issues_homogeneouscoverage_jpgs
            # collecting issues with several pngs
            elif pages_with_tifs == 0 and pages_with_one_pngs != 0 and total_pngs == total and pages_with_jpgs == 0:
                case = OriginalImageCase.issues_homogeneouscoverage_singlepngs
        else:
            case = OriginalImageCase.issues_missing_pageimg
        return issue_dir.path, case


def update_counter(counter, issue_path, case):
    counter[case.name].append(issue_path)


def print_report():
    logger.info(f'**** COUNTS ****')
    for k, v in original_counter.items():
        logger.info(f'{k}: {len(v)}')

    for c in OriginalImageCase:
        if (c == OriginalImageCase.issues_wo_zip
            or c == OriginalImageCase.issues_with_corruptedzip
            or c == OriginalImageCase.issues_missing_pageimg)\
                or c == OriginalImageCase.issues_fully_covered_pngs_with_isolated:
            paths = original_counter[c.name]
            if paths:
                logger.info(f'**** CASE {c.name} ****')
                for p in paths:
                    logger.info(f'\t{p}')


def init_logger(mylogger, log_level, log_file):
    """Initialise the logger."""
    mylogger.setLevel(log_level)

    if log_file is not None:
        handler = logging.FileHandler(filename=log_file, mode='w')
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )
    handler.setFormatter(formatter)
    mylogger.addHandler(handler)
    mylogger.info("Logger successfully initialised")


def run_check_canonical(global_report, local_report, original_issues, canonical_issues, parallel_execution):
    """
    Execute a sanity check of images from Olive by calling the function 'check_canonical'.

    The objective is to compare original and canonical image folders, and do a series of checks.
    See the README.md for more details.

    :param local_report:
    :param global_report:
    :param original_issues:
    :param canonical_issues:
    :param parallel_execution:
    :return:
    """

    # build original/canonical issue pairs
    pairs = path.pair_issue(original_issues, canonical_issues)

    # variables
    global canonical_cases
    global all_counts
    canonical_cases = initialize_case_dict()
    all_counts = initialize_stats_dict()

    # prepare the execution of the import function
    tasks = [
        delayed(check_canonical)(orig, can)
        for orig, can in pairs
    ]

    print(f"\nChecking {len(pairs)} issues pairs...(parallelized={parallel_execution})")
    logger.info(f"\nChecking {len(pairs)} issues pairs...(parallelized={parallel_execution})")

    # execute tasks
    results = utils.executetask(tasks, parallel_execution)
    print(str(all_counts))
    for cases, counter in results:
        canonical_cases.update(cases)
        for name, member in StatsImage.__members__.items():
            for c in counter:
                if c == member.value:
                    all_counts[member.value] += counter[c]
                    break
    print(str(all_counts))

    # handle results
    print(f"\nPrinting results")
    logger.info(f"\nPrinting results")

    # one line per journal, summary of cases in global report
    for name, member in CanonicalImageCase.__members__.items():
        for cc in canonical_cases:
            if cc == member.value:
                global_report.write(f"{len(canonical_cases[cc])}")
                break
        global_report.write(f", ")

    # continue on same line, image distribution in global report
    for name, member in StatsImage.__members__.items():
        for c in all_counts:
            if c == member.value:
                if c == "size jp2":
                    global_report.write(f"{humanize.naturalsize(all_counts[c])}")
                else:
                    global_report.write(f"{all_counts[c]}")
                break
        global_report.write(f", ")

    global_report.write("\n")

    # one file per journal, list of issues/pages per cases in detailed report
    for name, member in CanonicalImageCase.__members__.items():
        for cc in canonical_cases:
            if cc == member.value:
                local_report.write(f"\n**** CASE: {cc} ({len(canonical_cases[cc])}) **** \n")
                for issue in canonical_cases[cc]:
                    local_report.write(f"{issue}\n")
                break
    print(f"Done")


def main(args):
    """Execute the main with CLI parameters."""

    # store CLI parameters
    orig_dir = args["--original-dir"]
    canon_dir = args["--canonical-dir"]
    newspapers = args["--newspapers"]
    rep_dir = args["--report-dir"]
    log_file = args["--log-file"]
    command = args["--command"]
    parallel_execution = args["--parallelize"]
    log_level = logging.DEBUG if args["--verbose"] else logging.INFO

    # logger and some vars
    global logger
    init_logger(logger, log_level, log_file)
    logger.info(f"CLI arguments received: {args}")
    timestr = time.strftime("%Y%m%d-%H%M%S")

    # get titles
    journals = newspapers.split(" ")
    logger.info(f"Will consider the following newspapers: {journals}")

    # execution
    if command == 'check_original':

        # detect original issues to consider
        original_issues = path.detect_journal_issues(orig_dir, journals)
        logger.info(f"Found {len(original_issues)} newspaper issues to import")
        logger.debug(f"Following issues will be imported: {original_issues}")

        logger.info("Executing: check_original")

        # prepare the execution of the import function
        tasks = [
            delayed(check_original)(i, rep_dir)
            for i in original_issues
        ]

        print(
            "\nImporting {} newspaper issues...(parallelized={})".format(
                len(original_issues),
                parallel_execution
            )
        )
        # execute and print report
        result = utils.executetask(tasks, parallel_execution)
        for k, v in result:
            update_counter(original_counter, k, v)
        print_report()

    elif command == 'check_canonical':

        # prepare global report
        f_globalreport = os.path.join(rep_dir, "_".join(["globalreport", timestr, ".csv"]))
        fh_globalreport = open(f_globalreport, 'w')

        # header
        fh_globalreport.write(f"JOURNAL")
        for name, member in CanonicalImageCase.__members__.items():
            fh_globalreport.write(f" , {member.value}")
        for name, member in StatsImage.__members__.items():
            fh_globalreport.write(f" , {member.value}")
        fh_globalreport.write(f"\n")

        # sanity check for each journal
        for journal in journals:
            print(f"Executing: check_canonical for journal {journal}")
            logger.info(f"Executing: check_canonical for journal {journal}")

            # detect original issues to consider for current journal
            original_issues = path.detect_journal_issues(orig_dir, journal)
            logger.info(f"Found {len(original_issues)} original issues to check")
            print(f"Found {len(original_issues)} original issues to check")
            logger.debug(f"Following issues will be checked:\n")
            for i in original_issues:
                logger.debug(f"{i.path}")

            # detect canonical issues to consider
            canonical_issues = path.detect_canonical_issues(canon_dir, journal)
            logger.info(f"Found {len(canonical_issues)} canonical issues to check")
            print(f"Found {len(canonical_issues)} canonical issues to check")
            logger.debug(f"Following issues will be checked: {canonical_issues}")

            # journal report file
            f_localreport = os.path.join(rep_dir, "_".join([journal, "report", timestr]))
            fh_localreport = open(f_localreport, 'w')
            fh_localreport.write(f"======= REPORT for {journal} ======\n")

            fh_globalreport.write(f"{journal}, ")

            # check
            run_check_canonical(fh_globalreport, fh_localreport, original_issues, canonical_issues, parallel_execution)

        fh_globalreport.close()


if __name__ == "__main__":
    arguments = docopt.docopt(__doc__)
    main(arguments)

# Executions on cluster:
# for Le Temps:
# check_images.py --original-dir=/mnt/impresso_syno --canonical-dir=/mnt/project_impresso/images --report-dir=../reports --newspapers="01_GDL 02_GDL 01_JDG 02_JDG LNQ" --command="check_canonical" --log-file=../logs/check-canonical-letemps.log

