#!/usr/bin/env python
# coding: utf-8 
# created on 2018.03.27 using PyCharm 
# project impresso-image-acquisition

"""
Impresso project: Sanity check for images, original and canonical

Usage:
    check-image.py --command==<c> --newspapers=<np> --original-dir=<od> [--canonical-dir=<cd>  --report-dir==<rd>  --log-file=<lf> --verbose --parallelize]

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
from collections import defaultdict
import glob
from dask import delayed
from enum import Enum
import time
import humanize
import json

from impresso_commons.images import img_utils
import impresso_commons.path as path
import impresso_commons.utils as utils

logger = logging.getLogger(__name__)
original_counter = defaultdict(list)

__author__ = "maudehrmann"


class OriginalImageCase(Enum):
    """ Possible cases w.r.t. image format distribution in Olive archives."""
    issues_wo_zip = 'issues w/o zip'
    issues_with_corruptedzip = 'issues w/ corruptedzip'
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
    jp2_wrongdimensions = 'jp2_wrongdimensions'


class CanonicalImageStats(Enum):
    """ Image-related stats in impresso canonical archives."""
    number_tif = "number tif"
    number_png = "number png"
    number_jpg = "number jpg"
    size_jp2 = "total size jp2"
    number_original_pagefolder = "number original page folders"
    number_canonical_jp2 = "number canonical jp2"


class CanonicalJournalStats(Enum):
    """ Journal-related stats in impresso canonical archives."""
    issues_orig = "number original issues"
    issues_canon = "number canonical issues"
    issues_pairs = "number recognized pairs"
    issues_wo_zip = 'issues w/o zip'
    issues_with_corruptedzip = 'issues w/ corruptedzip'


class OriginalJournalStats(Enum):
    """ Journal-related stats in Olive original archives."""
    issues_orig = "number original issues"
    issues_valid = "number valid original issues"
    issues_with_large_pdf = 'issues w large pdf'  # one pdf per issue
    issues_with_small_pdfs = 'issues w small pdfs'  # one pdf per page
    number_pages = "number of pages"
    number_tif = "number tif"
    number_png = "number png"
    number_jpg = "number jpg"


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


def initialize_dict(enum_key, def_value):
    d = {}
    for enum_name, enum_member in enum_key.__members__.items():
        d.setdefault(enum_member.value, def_value)
    return d


def check_image_pairs(page_folders, jp2):
    """
    Check whether each image of page_folders have a corresponding jp2 one.

    :param page_folders: list which contains full paths to page folder (.../GDL/1900/01/10/1)
    :type page_folders: array.py
    :param jp2: list which contains full paths to jp2 images (.../GDL/1900/01/10/a/GDL-1900-01-10-a-p0001.jp2)
    :type jp2: list
    :return: list of pages (paths) without a jp2 twin
    :rtype: list
    """
    d = {}
    res = []

    # put jp2 in hash table
    for img in jp2:
        i = img[-8:-4]
        d[i] = ""
    # check if original pages have a jp2
    for page in page_folders:
        x = page.split('/')[-1]
        p = x.zfill(4)
        if p not in d:
            res.append(page)
    return res


def check_canonical_issue(issue_dir_original, issue_dir_canonical):
    """ Parses the impresso original and canonical image directories and detects anomalies.

    :param issue_dir_original: the original issue in the source folder
    :type issue_dir_original: IssueDir
    :param issue_dir_canonical: the canonical issue in the working folder
    :type issue_dir_canonical: IssueDir
    :return: a tuple consisting of: \n
            1. dictionary with k = L{CanonicalImageCase} v = issue or page of that case and \n
            2. dictionary keeping the counts of source image formats (how many were generated from tif, jpg etc.)
    :rtype: tuple

    Additional info
    ===============

    The function works at issue level.
    It detects the following:
        - when original page number does not correspond to canonical jp2 number
        - when there is no jp2
        - when there is no info file, or more than one
        - when the number of jp2 registered in info file differs fro jp2 number
        - when info/jp2 files do not comply to naming conv. and/or do not contain journal and/or issue date.

    It stores the following:
        - total size of jp2 in the issue
        - how many images where converted from which format

    This information is added up at journal level by the L{run_check_canonical} function.
    """

    logger.debug(f"Checking the following issues: {issue_dir_original.path} and {issue_dir_canonical.path}")

    # variables
    short_cano = path.get_issueshortpath(issue_dir_canonical)

    # local_cases_dict = initialize_dict(CanonicalImageCase, [])  # keep the paths (values) of problematic 'CanonicalImageCase' (key)
    local_cases_dict = {}  # keep the paths (values) of problematic 'CanonicalImageCase' (key)
    local_stats_dict = initialize_dict(CanonicalImageStats, 0)  # keep counts of image formats
    page_folders = []
    shortinfo = ""

    # get original page folders (to compare with what is produced on the canonical side)
    working_archive = os.path.join(issue_dir_original.path, "Document.zip")
    if os.path.isfile(working_archive):
        try:
            archive = zipfile.ZipFile(working_archive)
            page_folders = img_utils.get_page_folders(archive)
            # store number of original page folders
            local_stats_dict[CanonicalImageStats.number_original_pagefolder.value] += len(page_folders)
        except zipfile.BadZipfile as e:
            logger.info(f"Bad zip file in {issue_dir_original.path}")

    # get canonical page material
    jp2 = glob.glob(os.path.join(issue_dir_canonical.path, "*.jp2"))
    info = glob.glob(os.path.join(issue_dir_canonical.path, "*.json"))

    # jp2 simple check
    if not jp2:
        local_cases_dict.setdefault(CanonicalImageCase.issues_wo_jp2.value, []).append(short_cano)
    else:
        # store number of jp2
        local_stats_dict[CanonicalImageStats.number_canonical_jp2.value] += len(jp2)
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
            local_stats_dict[CanonicalImageStats.size_jp2.value] += jp2_bytes

    # info simple check
    if not info:
        local_cases_dict.setdefault(CanonicalImageCase.issues_wo_infofile.value, []).append(short_cano)
    else:
        # short path of info file
        shortinfo = info[0][info[0].index(issue_dir_canonical.journal):]

        # check number of info file
        if len(info) != 1:
            local_cases_dict.setdefault(CanonicalImageCase.issues_with_wrongnumber_infofile.value, []).append(
                short_cano)

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

        # keep stats of source image format
        with open(info[0]) as f:
            info = json.load(f)
            for i in info:
                source = i["s"]
                if "tif" in source:
                    local_stats_dict[CanonicalImageStats.number_tif.value] += 1
                elif "png" in source:
                    local_stats_dict[CanonicalImageStats.number_png.value] += 1
                elif "jpg" in source:
                    local_stats_dict[CanonicalImageStats.number_jpg.value] += 1

                sd = i["s_dim"]
                dd = i["d_dim"]
                if sd != dd:
                    local_cases_dict.setdefault(CanonicalImageCase.jp2_wrongdimensions, []).append(
                        shortinfo)

            # check if same number of img reported in info file than in reality
            if len(info) != len(jp2):
                local_cases_dict.setdefault(CanonicalImageCase.infofile_with_wrongnumber_img.value, []).append(
                    shortinfo)
        # nb_lines = 0
        # with open(info[0]) as f:
        #     for line in f:
        #         nb_lines += 1
        #         if "tif" in line:
        #             local_stats_dict[CanonicalImageStats.number_tif.value] += 1
        #         elif "png" in line:
        #             local_stats_dict[CanonicalImageStats.number_png.value] += 1
        #         elif "jpg" in line:
        #             local_stats_dict[CanonicalImageStats.number_jpg.value] += 1
        #
        # # check if same number of img reported in info file than in reality
        # if nb_lines != len(jp2):
        #     local_cases_dict.setdefault(CanonicalImageCase.infofile_with_wrongnumber_img.value, []).append(
        #         shortinfo)
    return local_cases_dict, local_stats_dict


def check_original_issue(issue_dir_original):
    """Parse Olive images of a journal issue.

    :param issue_dir_original: the journal issue issue directory
    :type issue_dir_original: IssueDir
    """

    # variables
    short_orig = path.get_issueshortpath(issue_dir_original)
    local_originalimagecase = {}
    local_statsjournal = initialize_dict(OriginalJournalStats, 0)

    # get zip archive and check it
    working_archive = os.path.join(issue_dir_original.path, "Document.zip")

    if not os.path.isfile(working_archive):
        local_originalimagecase.setdefault(OriginalImageCase.issues_wo_zip.value, []).append(short_orig)
    else:
        try:
            archive = zipfile.ZipFile(working_archive)
        except zipfile.BadZipFile as e:
            local_originalimagecase.setdefault(OriginalImageCase.issues_with_corruptedzip.value, []).append(
                short_orig)
            logger.info(f"Bad zip file in {short_orig}")
            return local_originalimagecase, local_statsjournal

        # if archive ok, proceed:
        local_statsjournal[OriginalJournalStats.issues_valid.value] += 1

        # collect top dirs
        page_folders = img_utils.get_page_folders(archive)
        page_number = len(page_folders)
        local_statsjournal[OriginalJournalStats.number_pages.value] += page_number

        # collect images
        tifs = img_utils.get_img_from_archive(archive, "Res/PageImg", ".tif")
        pngs = img_utils.get_img_from_archive(archive, "/Img", ".png", "/Pg")
        jpgs = img_utils.get_img_from_archive(archive, "/Img", ".jpg", "/Pg")

        local_statsjournal[OriginalJournalStats.number_tif.value] += len(tifs)
        local_statsjournal[OriginalJournalStats.number_png.value] += len(pngs)
        local_statsjournal[OriginalJournalStats.number_jpg.value] += len(jpgs)

        # collect pdf
        ext = ["*.pdf", "*.PDF"]
        big_pdfs = []
        small_pdfs = []
        for e in ext:
            big_pdfs.extend(glob.glob(os.path.join(issue_dir_original.path, e)))
            small_pdfs.extend(glob.glob(os.path.join(issue_dir_original.path, "Res", "PDF", e)))

        if big_pdfs:
            local_statsjournal[OriginalJournalStats.issues_with_large_pdf.value] += 1
        if small_pdfs:
            local_statsjournal[OriginalJournalStats.issues_with_small_pdfs.value] += 1

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
                case = OriginalImageCase.issues_heterocoverage_all.value
            elif pages_with_tifs != 0 and pages_with_pngs != 0 and pages_with_jpgs == 0:
                case = OriginalImageCase.issues_heterocoverage_tif_png.value
            elif pages_with_tifs != 0 and pages_with_pngs == 0 and pages_with_jpgs != 0:
                case = OriginalImageCase.issues_heterocoverage_tif_jpg.value
            elif pages_with_tifs == 0 and pages_with_pngs != 0 and pages_with_jpgs != 0:
                case = OriginalImageCase.issues_heterocoverage_png_jpg.value
            # non mixed cases
            elif pages_with_tifs == total and pages_with_pngs == 0 and pages_with_jpgs == 0:
                case = OriginalImageCase.issues_homogeneouscoverage_tifs.value
            elif pages_with_tifs == 0 and pages_with_pngs == total and pages_with_jpgs == 0:
                case = OriginalImageCase.issues_homogeneouscoverage_pngs.value
            elif pages_with_tifs == 0 and pages_with_pngs == 0 and pages_with_jpgs == total:
                case = OriginalImageCase.issues_homogeneouscoverage_jpgs.value
            # collecting issues with several pngs
            elif pages_with_tifs == 0 and pages_with_one_pngs != 0 and total_pngs == total and pages_with_jpgs == 0:
                case = OriginalImageCase.issues_homogeneouscoverage_singlepngs.value
        else:
            case = OriginalImageCase.issues_missing_pageimg.value

        if case is not None:
            local_originalimagecase.setdefault(case, []).append(short_orig)
    return local_originalimagecase, local_statsjournal


def check_canonical_journal(original_issues, canonical_issues, parallel_execution):
    """
    Execute a sanity check of images from Olive by calling the function 'check_canonical'.

    The objective is to compare original and canonical image folders, and do a series of checks.
    See the README.md for more details.

    :param original_issues:
    :param canonical_issues:
    :param parallel_execution:
    :return:
    """

    # build original/canonical issue pairs
    pairs = path.pair_issue(original_issues, canonical_issues)

    # variables
    # canonical_cases = initialize_dict(CanonicalImageCase, [])
    global_canonical_cases = {}
    global_image_counts = initialize_dict(CanonicalImageStats, 0)
    global_journal_counts = initialize_dict(CanonicalJournalStats, 0)

    # counts at journal level
    global_journal_counts[CanonicalJournalStats.issues_orig.value] = len(original_issues)
    global_journal_counts[CanonicalJournalStats.issues_canon.value] = len(canonical_issues)
    global_journal_counts[CanonicalJournalStats.issues_pairs.value] = len(pairs)

    # prepare the execution of the import function
    tasks = [
        delayed(check_canonical_issue)(orig, can)
        for orig, can in pairs
    ]

    print(f"\nChecking {len(pairs)} issues pairs...(parallelized={parallel_execution})")
    logger.info(f"\nChecking {len(pairs)} issues pairs...(parallelized={parallel_execution})")

    # execute tasks
    results = utils.executetask(tasks, parallel_execution)

    # add local (issue) results to global (journal) results
    for cases, stats in results:
        for name, member in CanonicalImageCase.__members__.items():
            for c in cases:
                if c == member.value:
                    global_canonical_cases.setdefault(member.value, []).append(cases[c][0])
                    break

        for name, member in CanonicalImageStats.__members__.items():
            for c in stats:
                if c == member.value:
                    global_image_counts[member.value] += stats[c]
                    break
    return global_canonical_cases, global_image_counts, global_journal_counts


def check_original_journal(original_issues, parallel_execution):
    """
    Execute a sanity check of images from Olive by calling the function 'check_canonical'.

    The objective is to compare original and canonical image folders, and do a series of checks.
    See the README.md for more details.

    :param original_issues:
    :param parallel_execution:
    :return:
    """

    # variables
    #global_original_cases = initialize_dict(OriginalImageCase, [])
    global_original_cases = {}
    global_journal_counts = initialize_dict(OriginalJournalStats, 0)

    # counts at journal level
    global_journal_counts[OriginalJournalStats.issues_orig.value] = len(original_issues)

    # prepare execution
    tasks = [
        delayed(check_original_issue)(orig)
        for orig in original_issues
    ]

    print(f"\nChecking {len(original_issues)} original issues...(parallelized={parallel_execution})")
    logger.info(f"\nChecking {len(original_issues)} original issues...(parallelized={parallel_execution})")

    # execute tasks
    results = utils.executetask(tasks, parallel_execution)

    # handle results
    for local_cases, local_stats in results:
        for name, member in OriginalImageCase.__members__.items():
            for c in local_cases:
                if c == member.value:
                    global_original_cases.setdefault(member.value, []).append(local_cases[c][0])
                    break

        for name, member in OriginalJournalStats.__members__.items():
            for c in local_stats:
                if c == member.value:
                    global_journal_counts[member.value] += local_stats[c]
                    break
    return global_original_cases, global_journal_counts


def print_canonicalreport(canonical_cases, image_counts, journal_counts, global_report, local_report):
    """

    @param canonical_cases:
    @param image_counts:
    @param journal_counts:
    @param global_report:
    @param local_report:
    @return:
    """
    # handle results
    logger.info(f"\nPrinting results")

    # Print global report
    # one line per journal, number of issues
    for name, member in CanonicalJournalStats.__members__.items():
        for jc in journal_counts:
            if jc == member.value:
                global_report.write(f"{journal_counts[jc]}")
                break
        global_report.write(f", ")

    # one line per journal, summary of cases in global report
    for name, member in CanonicalImageCase.__members__.items():
        for cc in canonical_cases:
            if cc == member.value:
                global_report.write(f"{len(canonical_cases[cc])}")
                break
        global_report.write(f", ")

    # continue on same line, image distribution in global report
    for name, member in CanonicalImageStats.__members__.items():
        for ic in image_counts:
            if ic == member.value:
                if ic == CanonicalImageStats.size_jp2.value:
                    global_report.write(f"{humanize.naturalsize(image_counts[ic])}")
                else:
                    global_report.write(f"{image_counts[ic]}")
                break
        global_report.write(f", ")

    global_report.write("\n")

    # Print local report
    # one file per journal, list of issues/pages per cases in detailed report
    for name, member in CanonicalImageCase.__members__.items():
        for cc in canonical_cases:
            if cc == member.value:
                local_report.write(f"\n**** CASE: {cc} ({len(canonical_cases[cc])}) **** \n")
                for issue in canonical_cases[cc]:
                    local_report.write(f"{issue}\n")
                break


def print_originalreport(global_original_cases, global_journal_counts, global_report, local_report):
    """
    @param global_original_cases:
    @param global_journal_counts:
    @param global_report:
    @param local_report:
    @return:
    """
    # print results
    print(f"\nPrinting results")
    logger.info(f"\nPrinting results")

    # Print global report
    # one line per journal, number of issues
    for name, member in OriginalJournalStats.__members__.items():
        for jc in global_journal_counts:
            if jc == member.value:
                global_report.write(f"{global_journal_counts[jc]}")
                break
        global_report.write(f", ")

    # continue on same line, image distribution in global report
    for name, member in OriginalImageCase.__members__.items():
        for ic in global_original_cases:
            if ic == member.value:
                global_report.write(f"{len(global_original_cases[ic])}")
                break
        global_report.write(f", ")

    global_report.write("\n")

    # Print local report
    # one file per journal, list of issues/pages per cases in detailed report
    for name, member in OriginalImageCase.__members__.items():
        for oic in global_original_cases:
            if oic == member.value:
                local_report.write(f"\n**** CASE: {oic} ({len(global_original_cases[oic])}) **** \n")
                for issue in global_original_cases[oic]:
                    local_report.write(f"{issue}\n")
                break


def run_check_canonical(command, journals, orig_dir, canon_dir, report_dir, parallel_execution):
    """

    @param canon_dir:
    @param command:
    @param journals:
    @param orig_dir:
    @param report_dir:
    @param parallel_execution:
    @return:
    """

    # TODO: add warning in log if number of pages and images differ.

    # variables
    timestr = time.strftime("%Y%m%d-%H%M%S")

    # prepare global report
    f_globalreport = os.path.join(report_dir, "_".join(["canonical_globalreport", timestr, ".csv"]))
    fh_globalreport = open(f_globalreport, 'w')

    # header of global report
    fh_globalreport.write(f"JOURNAL")
    for name, member in CanonicalJournalStats.__members__.items():
        fh_globalreport.write(f" , {member.value}")
    for name, member in CanonicalImageCase.__members__.items():
        fh_globalreport.write(f" , {member.value}")
    for name, member in CanonicalImageStats.__members__.items():
        fh_globalreport.write(f" , {member.value}")
    fh_globalreport.write(f"\n")

    # sanity check for each journal
    for journal in journals:
        print(f"\n*** Executing: {command} for journal {journal}:")
        logger.info(f"\n*** Executing: {command} for journal {journal}:")

        # detect issues to consider for current journal
        original_issues = path.detect_journal_issues(orig_dir, journal)
        canonical_issues = path.detect_canonical_issues(canon_dir, [journal])
        print(f"{canon_dir} {journal}")

        logger.info(f"Found {len(original_issues)} original and {len(canonical_issues)} canonical issues to check")
        print(f"Found {len(original_issues)} original and {len(canonical_issues)} canonical issues to check")

        # report files
        f_localreport = os.path.join(report_dir, "_".join([journal, "canonical_report", timestr]))
        fh_localreport = open(f_localreport, 'w')
        fh_localreport.write(f"======= REPORT for {journal} ======\n")
        fh_globalreport.write(f"{journal}, ")

        # check
        canonical_cases, image_counts, journal_counts = check_canonical_journal(original_issues, canonical_issues, parallel_execution)
        print_canonicalreport(canonical_cases, image_counts, journal_counts, fh_globalreport, fh_localreport)

    fh_globalreport.close()
    print(f"Done")


def run_check_original(command, journals, orig_dir, report_dir, parallel_execution):
    """

    @param command:
    @param journals:
    @param orig_dir:
    @param report_dir:
    @param parallel_execution:
    @return:
    """

    # variables
    timestr = time.strftime("%Y%m%d-%H%M%S")

    # prepare global report
    f_globalreport = os.path.join(report_dir, "_".join(["original_globalreport", timestr, ".csv"]))
    fh_globalreport = open(f_globalreport, 'w')

    # header of global report
    fh_globalreport.write(f"journal")
    for name, member in OriginalJournalStats.__members__.items():
        fh_globalreport.write(f" , {member.value}")
    for name, member in OriginalImageCase.__members__.items():
        fh_globalreport.write(f" , {member.value}")
    fh_globalreport.write(f"\n")

    # sanity check for each journal
    for journal in journals:
        print(f"Executing: {command} for journal {journal}")
        logger.info(f"Executing: {command} for journal {journal}")

        # detect issues to consider for current journal
        original_issues = path.detect_journal_issues(orig_dir, journal)
        logger.info(f"Found {len(original_issues)} original issues to check for journal {journal}")
        print(f"Found {len(original_issues)} original  issues to check for journal {journal}")

        # local report file
        f_localreport = os.path.join(report_dir, "_".join([journal, "original_report", timestr]))
        fh_localreport = open(f_localreport, 'w')
        fh_localreport.write(f"======= REPORT for {journal} ======\n")
        fh_globalreport.write(f"{journal}, ")

        # check
        journal_original_cases, journal_counts = check_original_journal(original_issues, parallel_execution)

        # print results
        print_originalreport(journal_original_cases, journal_counts, fh_globalreport, fh_localreport)

    fh_globalreport.close()
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

    # get titles
    journals = newspapers.split(" ")
    logger.info(f"Will consider the following newspapers: {journals}")

    # execution
    if command == 'check_original':
        logger.info(f"Executing: {command}")
        run_check_original(command, journals, orig_dir, rep_dir, parallel_execution)

    elif command == 'check_canonical':
        logger.info(f"Executing: {command}")
        run_check_canonical(command, journals, orig_dir, canon_dir, rep_dir, parallel_execution)


if __name__ == "__main__":
    arguments = docopt.docopt(__doc__)
    main(arguments)

# Executions on cluster:
# ORIGINAL batch 1
# check_images.py --original-dir=/mnt/project_impresso/original/RERO --report-dir=../../reports/img_original --newspapers="BDC CDV EDA EXP JDV LCE LES LSR" --command="check_original" --log-file=../../logs/check-original-rero-batch1.log

# ORIGINAL batch 2
# check_images.py --original-dir=/mnt/project_impresso/original/RERO --report-dir=../../reports/img_original --newspapers="DLE IMP JDF LBP LCG LCR LCS LNF LSE LTF LVE" --command="check_original" --log-file=../../logs/check-original-rero-batch2.log

# ORIGINAL batch 1 & 2
# ./check_images.py --original-dir=/mnt/project_impresso/original/RERO --report-dir=../../reports/img_original --newspapers="BDC CDV EDA EXP JDV LCE LES LSR DLE IMP JDF LBP LCG LCR LCS LNF LSE LTF LVE" --command="check_original" --log-file=../../logs/check-original-rero-batch1-2.log

# ORIGINAL LeTemps
# check_images.py --original-dir=/mnt/impresso_syno --report-dir=../../reports/img_original --newspapers="01_GDL 02_GDL 01_JDG 02_JDG 02_LNQ" --command="check_original" --log-file=../../logs/check-original-letemps.log

# CANONICAL batch 1 & 2
# check_images.py --original-dir=/mnt/project_impresso/original/RERO --canonical-dir=/mnt/project_impresso/images --report-dir=../../reports/img_canonical --newspapers="BDC CDV EDA EXP JDV LCE LES LSR DLE IMP JDF LBP LCG LCR LCS LNF LSE LTF LVE" --command="check_canonical" --log-file=../../logs/check-canonical-rero-batch1-2.log

# CANONICAL Le Temps
# check_images.py --original-dir=/mnt/impresso_syno --canonical-dir=/mnt/project_impresso/images --report-dir=../../reports/img_canonical --newspapers="01_GDL 02_GDL 01_JDG 02_JDG 02_LNQ" --command="check_canonical" --log-file=../../logs/check-canonical-letemps.log


