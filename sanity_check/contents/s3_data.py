"""Functions to fetch impresso data from S3 storage."""

from impresso_commons.utils.s3 import get_s3_client, IMPRESSO_STORAGEOPT
from impresso_commons.utils.s3 import fixed_s3fs_glob
from dask import bag as db
import json
import os

S3_CANONICAL_DATA_BUCKET = "s3://original-canonical-fixed"
S3_REBUILT_DATA_BUCKET = "s3://canonical-rebuilt"


def list_newspapers(
    bucket_name=S3_CANONICAL_DATA_BUCKET,
    s3_client=get_s3_client()
):
    """List newspapers contained in an s3 bucket with impresso data."""
    print(f'Fetching list of newspapers from {bucket_name}')

    if "s3://" in bucket_name:
        bucket_name = bucket_name.replace("s3://", "").split("/")[0]

    paginator = s3_client.get_paginator('list_objects')

    newspapers = set()
    for n, resp in enumerate(paginator.paginate(
        Bucket=bucket_name,
        PaginationConfig={'PageSize': 10000}
    )):
        for f in resp['Contents']:
            newspapers.add(f["Key"].split("/")[0])
    print(f'{bucket_name} contains {len(newspapers)} newspapers')
    return newspapers


def list_issues(bucket_name=S3_CANONICAL_DATA_BUCKET):
    if bucket_name:
        newspapers = list_newspapers(bucket_name)
    else:
        newspapers = list_newspapers()
    issue_files = [
        file
        for np in newspapers
        for file in fixed_s3fs_glob(
            f"{os.path.join(bucket_name, f'{np}/issues/*')}"
        )
    ]
    print(f'{bucket_name} contains {len(issue_files)} .bz2 files')
    return issue_files


def list_pages(bucket_name=S3_CANONICAL_DATA_BUCKET):
    if bucket_name:
        newspapers = list_newspapers(bucket_name)
    else:
        newspapers = list_newspapers()

    page_files = db.from_sequence(newspapers).map(
        lambda np: fixed_s3fs_glob(
            f"{os.path.join(bucket_name, f'{np}/pages/*')}"
        )
    ).flatten().compute()
    print(f'{bucket_name} contains {len(page_files)} .bz2 files')
    return page_files


def list_files_rebuilt(bucket_name=S3_REBUILT_DATA_BUCKET):
    if bucket_name:
        newspapers = list_newspapers(bucket_name)
    else:
        newspapers = list_newspapers()
    rebuilt_files = [
        file
        for np in newspapers
        for file in fixed_s3fs_glob(
            f"{os.path.join(bucket_name, f'{np}/*')}"
        )
    ]
    print(f'{bucket_name} contains {len(rebuilt_files)} .bz2 files')
    return rebuilt_files


def fetch_issue_ids_rebuilt(bucket_name=S3_REBUILT_DATA_BUCKET, compute=True):
    """
    Derive issue IDs from an s3 bucket with rebuilt data.

    Since rebuilt data is organized by content item and not by issue, we need
    to parse all content items IDs in rebuilt data and derive issue IDs.
    """
    rebuilt_files = list_files_rebuilt(bucket_name)
    ci_bag = db.read_text(
        rebuilt_files,
        storage_options=IMPRESSO_STORAGEOPT
    ).map(
        json.loads
    ).map(
        lambda ci: '-'.join(ci['id'].split('-')[:-1])
    ).distinct()

    if compute:
        return ci_bag.compute()
    else:
        return ci_bag


def fetch_issues(bucket_name=S3_CANONICAL_DATA_BUCKET, compute=True):
    """
    Fetch issue JSON docs from an s3 bucket with impresso canonical data.
    """
    issue_files = list_issues(bucket_name)

    print((
        f'Fetching issue ids from {len(issue_files)} .bz2 files '
        f'(compute={compute})'
    ))
    issue_bag = db.read_text(
        issue_files,
        storage_options=IMPRESSO_STORAGEOPT
    ).map(json.loads)

    if compute:
        return issue_bag.compute()
    else:
        return issue_bag


def fetch_issue_ids(
    bucket_name=S3_CANONICAL_DATA_BUCKET,
    compute=True,
    issue_bag=None
):
    """
    Fetch newspaper issue IDs from an s3 bucket with impresso canonical data.
    """
    if not issue_bag:
        issue_bag = fetch_issues(bucket_name, compute=False)
    else:
        print(f'using input issue bag {issue_bag}')

    issue_id_bag = issue_bag.pluck('id')

    if compute:
        return issue_id_bag.compute()
    else:
        return issue_id_bag


# TODO:
# - add  possibility to do it only for certain newspapers
# - finish implementation
def fetch_page_ids(
    bucket_name=S3_CANONICAL_DATA_BUCKET,
    source="issues",
    issue_bag=None
):

    valid_sources = ["issues", "pages"]
    assert source in valid_sources

    if issue_bag is None:
        issue_bag = fetch_issues(
            bucket_name,
            compute=False
        ).filter(lambda i: len(i) > 0)

    if source == "issues":

        print(f'Fetching page IDs from {source}')

        # no need to recompute the issues
        if issue_bag:
            pass
        else:
            issue_bag = fetch_issues(compute=False)
        return issue_bag.map(lambda i: i['pp']).flatten().compute()
    else:
        pass
