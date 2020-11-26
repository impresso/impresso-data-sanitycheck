"""Command-line script to perform sanity check comparing the number of local issues against s3.

Usage:
    check_imported_issues.py --canonical-bucket=<cb> --local-dirs=<list> [--output-dir=<od> --thres=<float> --workers=<int>]

Options:

--canonical-bucket=<cb>     S3 bucket from where the canonical JSON data will be read.
--local-dirs=<list>         Local directories where the original data is stored (list comma-separated).
--output-dir=<od>           Directory where the results are stored.
--thres=<float>             Threshold for indicating mismatches [default: 1.0].
--workers=<int>             Number of parallel Dask workers [default: 8].

Example:

    python sanity_check/contents/check_imported_issues.py --canonical-bucket='s3://canonical-data' \
    --local-dirs "/mnt/project_impresso/original/UZH \
    /mnt/project_impresso/original/BNL /mnt/project_impresso/original/RERO \
    /mnt/project_impresso/original/RERO2 /mnt/project_impresso/original/RERO3 /mnt/impresso_syno \
    /mnt/project_impresso/original/BNF /mnt/project_impresso/original/BNF-EN \
    /mnt/project_impresso/original/BL /mnt/project_impresso/original/SWA" \
    --output-dir=logs_sanity_check/ \
    --thres 1.0 \
    --workers 8
"""

import logging
import os
from datetime import datetime

from docopt import docopt

import dask
from dask import bag as db
from dask import dataframe as dd
from dask import array as da

from sanity_check.contents.s3_data import fetch_issue_ids

from impresso_commons.path.path_fs import detect_issues
from text_importer.importers.rero.detect import detect_issues as rero_detect_issues
from text_importer.importers.lux.detect import detect_issues as lux_detect_issues
from text_importer.importers.bnf.detect import detect_issues as bnf_detect_issues
from text_importer.importers.bnf_en.detect import detect_issues as bnfen_detect_issues, dir2issue, BnfEnIssueDir
from text_importer.importers.bl.detect import detect_issues as bl_detect_issues
from text_importer.importers.swa.detect import detect_issues as swa_detect_issues

from dask.distributed import Client




def bnfen_custom_detect_issues(base_dir: str, access_rights: str = None):
    """Detect newspaper issues to import within the filesystem.
    This function expects the directory structure that BNF-EN used to
    organize the dump of Mets/Alto OCR data.
    :param str base_dir: Path to the base directory of newspaper data.
    :param str access_rights: Not used for this imported, but argument is kept for normality
    :return: List of `BnfEnIssueDir` instances, to be imported.
    """

    dir_path, dirs, files = next(os.walk(base_dir))
    journal_dirs = [os.path.join(dir_path, _dir) for _dir in dirs]
    issue_dirs = [
        os.path.join(journal, _dir)
        for journal in journal_dirs
        for _dir in os.listdir(journal)
    ]

    issue_dirs = [bnfen_dir2issue(_dir, None) for _dir in issue_dirs]

    issue_dirs = [i for i in issue_dirs if i is not None]

    return issue_dirs


def bnfen_dir2issue(path: str, access_rights: dict):
    """Create a `BnfEnIssueDir` object from a directory path.
    .. note ::
        This function is called internally by :func:`detect_issues`
    :param str path: Path of issue.
    :return: New ``BnfEnIssueDir`` object
    """
    journal, issue = path.split('/')[-2:]

    date, edition = issue.split('_')[:2]
    date = datetime.strptime(date, '%Y%m%d').date()
    journal = journal.lower().replace('-', '').strip()
    edition = 'X'

    return BnfEnIssueDir(journal=journal, date=date, edition=edition, path=path,
                         rights="open-public", ark_link="IIIF_LINK")


def detect_issues_from_dirs(local_dirs: list):
    """
    Wrapper to detect issues for various sources and file structures
    """

    issues = []

    for path in local_dirs:
        path_lower = path.lower()
        if 'rero2' in path_lower:
            issues += rero_detect_issues(path, "/mnt/project_impresso/original/RERO2/rero2_access_rights.json")
        elif 'rero3' in path_lower:
            issues += rero_detect_issues(path, "/mnt/project_impresso/original/RERO3/access_rights.json")
        elif 'bnl' in path_lower:
            issues += lux_detect_issues(path)
        elif 'bnf-en' in path_lower:
            issues += bnfen_custom_detect_issues(path)
        elif 'bnf' in path_lower:
            issues += bnf_detect_issues(path, "/mnt/project_impresso/original/BNF/access_rights.json")
        elif 'bl' in path_lower:
            issues += bl_detect_issues(path, access_rights=None, tmp_dir='tmp_bnl_uncompressed')
        elif 'swa' in path_lower:
            issues += swa_detect_issues(path, "/mnt/project_impresso/original/SWA/access_rights.json")
        else:
            issues += detect_issues(path)

    return issues


def canonical_issue_meta_from_id(issue_id):
    journal, year, month, day, edition = issue_id.split('-')
    meta = {"journal": journal, "year": year, "issue_id": issue_id}

    return meta


def canonical_issue_name(issues):
    """
    Create a canonical issue id from an `IssueDir` object.
    """

    ret = []

    for issue in issues:
        issue_id = "-".join(
            [
                issue.journal,
                str(issue.date.year),
                str(issue.date.month).zfill(2),
                str(issue.date.day).zfill(2),
                issue.edition
            ]
        )

        ret.append([issue.journal, issue.date.year, issue_id])

    return ret


def aggr_by_year_journal(df: dask.dataframe) -> dask.dataframe:
    """Aggregate issue count by year and newspaper.

    :param dask.dataframe df: Dataframe comprising all issues.
    :return: Dataframe grouped by year and source .
    :rtype: dask.dataframe

    """

    return df.groupby(['journal', 'year']).count()


def filter_sources_with_mismatch(df: dask.dataframe, thres=1.0) -> dask.dataframe:
    """Filter dataframe for issue coverage below a the threshold.

    :param dask.dataframe df: Dataframe comprising number of local and s3 issues.
    :param type thres: Threshold for issue coverage.
    :return: Filtered dataframe.
    :rtype: dask.dataframe

    """


    df['coverage'] = None
    df['coverage'] = df['n_issues_s3'] / df['n_issues_local']
    df = df[(df.coverage < thres) | (df.coverage.isna())]

    return df


def run_issue_comparison(s3_bucket: str, local_dirs: list) -> dask.dataframe:
    """Collect all local issues and issues from s3 for comparison.

    :param str s3_bucket: Bucket on s3.
    :param list local_dirs: Directories where the original data is stored.
    :return: Overview of imported issues.
    :rtype: dask.dataframe

    """

    logging.info('Collecting local issues.')

    issues_local = db.from_sequence(local_dirs, partition_size=1) \
        .map_partitions(detect_issues_from_dirs) \
        .compute()

    df_local = db.from_sequence(issues_local, npartitions=30) \
        .map_partitions(canonical_issue_name) \
        .to_dataframe(meta={'journal': str, 'year': int, 'issue_id': str}) \
        .compute()


    df_n_issues_local = aggr_by_year_journal(df_local) \
        .rename(columns={"issue_id": "n_issues_local"})


    logging.info('Collecting issues from s3.')

    s3_canonical_issue_ids = fetch_issue_ids(bucket_name=s3_bucket)

    df_s3 = db.from_sequence(s3_canonical_issue_ids) \
        .map(canonical_issue_meta_from_id) \
        .to_dataframe(meta={'journal': str, 'year': int, 'issue_id': str}) \
        .compute()

    df_n_issues_s3 = aggr_by_year_journal(df_s3) \
        .rename(columns={"issue_id": "n_issues_s3"})

    df_comb = df_n_issues_local.merge(df_n_issues_s3, how="outer", left_index=True, right_index=True).reset_index()

    return df_comb


def run_checks_imported_issues(s3_bucket:str, local_dirs:list, thres:int, output_dir:str=None):

    logging.info(f"Provided s3 bucket: {s3_bucket}")
    logging.info(f"Provided local resources: {' '.join(local_dirs)}")

    logging.info('Start comparing the number of issues between local and s3, grouped by newspaper and year.')

    df_comb = run_issue_comparison(s3_bucket, local_dirs)
    df_err = filter_sources_with_mismatch(df_comb, thres=thres)

    logging.info('Done.')

    if output_dir:

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        f_out_mismatch = 'data_ingestion_issue_mismatch.csv'
        f_out_overview = 'data_ingestion_issue_overview.csv'

        csv_path_mismatch = os.path.join(output_dir, f_out_mismatch)
        csv_path_overview = os.path.join(output_dir, f_out_overview)

        df_comb.to_csv(csv_path_overview)
        df_err.to_csv(csv_path_mismatch)

        logging.info(f"Written CSV containing all mismatches to {csv_path_mismatch}.")
        logging.info(f"Written CSV containing all issues to {csv_path_overview}.")


def main():
    arguments = docopt(__doc__)
    s3_canonical_bucket = arguments["--canonical-bucket"]
    local_dirs = arguments["--local-dirs"].split()
    thres = float(arguments["--thres"])
    output_dir = arguments["--output-dir"]
    workers = int(arguments["--workers"]) if arguments["--workers"] else 8


    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    try:
        # create a dask client
        dask_client = Client(n_workers=workers)

        # NB here we check that scheduler and workers do have the same
        # versions of the various libraries, as mismatches may cause
        # exceptions and weird behaviours.
        libraries_versions = dask_client.get_versions(check=True)
        logging.info(dask_client)

        run_checks_imported_issues(s3_bucket=s3_canonical_bucket, local_dirs=local_dirs, output_dir=output_dir, thres=thres)

    except Exception as e:
        raise e


if __name__ == "__main__":
    main()
