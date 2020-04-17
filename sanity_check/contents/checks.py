"""Command-line script to perform sanity checks on canonical/rebuilt data in s3.

Usage:
    checks.py --canonical-bucket=<cb> --rebuilt-bucket=<rb> --output-dir=<od> [--k8-memory=<mem> --k8-workers=<wkrs>]

Options:

--canonical-bucket=<cb>  S3 bucket where canonical JSON data will be read from

Example:

    python sanity_check/contents/sync.py s3 --canonical-bucket='s3://original-canonical-staging' \
    --rebuilt-bucket='s3://passim-rebuilt' --output-dir=./ --k8-memory="1G" --k8-workers=25
"""  # noqa: E501

from collections import Counter
from dask_k8 import DaskCluster
from docopt import docopt

from impresso_commons.utils.kube import (
    make_scheduler_configuration,
    make_worker_configuration,
)
from s3_data import fetch_issues, fetch_page_ids, fetch_issue_ids
import pandas as pd
from dask import bag
import os


def find_duplicated_content_item_IDs(issue_json: dict) -> list:
    """
    Find duplicated content item IDs in the ToC of a newspaper issue.
    """
    ci_ids = [ci['m']['id'] for ci in issue_json['i']]
    duplicates = []
    for ci_id, count in Counter(ci_ids).items():
        if count > 1:
            duplicates.append(ci_id)
    return duplicates


# TODO: this is a local check, but we need to do also a global check
def check_duplicated_content_item_IDs(issue_bag: bag) -> pd.DataFrame:
    duplicates_bag = issue_bag.map(find_duplicated_content_item_IDs).flatten()

    duplicates = duplicates_bag.map(
        lambda i: {
            "id": i,
            "issue_id": "-".join(i.split('-')[:-1]),
            "newspaper_id": i.split('-')[0],
            "year": int(i.split('-')[1]),
        }
    ).compute()

    if duplicates:
        duplicates_df = pd.DataFrame(duplicates).set_index('id')
    else:
        # there are no duplicates
        duplicates_df = pd.DataFrame(columns=['id', 'issue_id', 'newspaper_id', 'year'])

    print(
        (
            f'Found {duplicates_df.shape[0]} duplicated '
            'content item IDs, belonging to '
            f'{duplicates_df.newspaper_id.unique().size} journals.'
        )
    )
    return duplicates_df


def check_duplicated_issues_IDs(issue_bag):
    """Check that newspaper issue IDs are unique within the corpus."""
    issue_ids = fetch_issue_ids(issue_bag=issue_bag)
    print(f'{len(issue_ids)} issue IDs were fetched')

    duplicate_issue_ids = []
    for issue_id, count in Counter(issue_ids).items():
        if count > 1:
            duplicate_issue_ids.append(issue_id)
    print(f'{len(duplicate_issue_ids)} duplicated IDs were found')
    return duplicate_issue_ids


# TODO: test
def check_inconsistent_page_ids(canonical_bucket_name: str) -> pd.DataFrame:
    """Check whether there are mismatches between page IDs.

    Page IDs are found in two places in our data:
    1. field `pp` of issue JSON
    2. field `id` of page JSON

    As there can be mismatches between these two sets of identifiers,
    we need to verify that all page IDs in #1 are contained within #2.
    """

    page_ids_from_issues = fetch_page_ids(canonical_bucket_name, source="issues")
    page_ids_from_pages = fetch_page_ids(canonical_bucket_name, source="pages")

    df_page_ids_from_issues = (
        bag.from_sequence(set(page_ids_from_issues))
        .map(lambda id: {"id": id, "from_issues": True})
        .to_dataframe()
        .set_index('id')
        .persist()
    )

    df_page_ids_from_pages = (
        bag.from_sequence(page_ids_from_pages)
        .map(lambda id: {"id": id, "from_pages": True})
        .to_dataframe()
        .set_index('id')
        .persist()
    )

    df_pages = df_page_ids_from_issues.join(df_page_ids_from_pages, how='outer').compute()

    df_pages['newspaper_id'] = df_pages.index.map(lambda z: z.split('-')[0])
    return df_pages[~(df_pages.from_pages == df_pages.from_issues)]


def run_checks_canonical(canonical_bucket_name, output_dir=None):
    canonical_issues_bag = fetch_issues(canonical_bucket_name, compute=False).filter(lambda i: len(i) > 0)

    # 1) verify that there are not duplicated content item IDs
    duplicates_df = check_duplicated_content_item_IDs(canonical_issues_bag)

    if output_dir and os.path.exists(output_dir):
        fname = "duplicate_ci_ids"
        duplicates_df.to_pickle(os.path.join(output_dir, f"{fname}.pkl"))
        duplicates_df.to_csv(os.path.join(output_dir, f"{fname}.csv"))

    # 2) verify the consistency of page IDs
    pages_df = check_inconsistent_page_ids(canonical_bucket_name)
    print(pages_df.head())

    if output_dir and os.path.exists(output_dir):
        fname = "inconsistent_page_ids"
        duplicates_df.to_pickle(os.path.join(output_dir, f"{fname}.pkl"))
        duplicates_df.to_csv(os.path.join(output_dir, f"{fname}.csv"))

    return duplicates_df, pages_df


def main():
    arguments = docopt(__doc__)
    s3_canonical_bucket = arguments['--canonical-bucket']
    s3_rebuilt_bucket = arguments['--rebuilt-bucket']
    output_dir = arguments['--output-dir']
    memory = arguments['--k8-memory'] if arguments['--k8-memory'] else "1G"
    workers = int(arguments['--k8-workers']) if arguments['--k8-workers'] else 50

    image_uri = "ic-registry.epfl.ch/dhlab/impresso_data-sanity-check:v1"

    try:
        # first thing to do is to create the dask kubernetes cluster
        cluster = DaskCluster(
            namespace="dhlab",
            cluster_id="impresso-sanitycheck-cli",
            scheduler_pod_spec=make_scheduler_configuration(),
            worker_pod_spec=make_worker_configuration(docker_image=image_uri, memory=memory),
        )
        cluster.create()
        cluster.scale(workers, blocking=True)
        dask_client = cluster.make_dask_client()
        dask_client.get_versions(check=True)
        print(dask_client)
        run_checks_canonical(s3_canonical_bucket, output_dir)

        # TODO: do stuff

    except Exception as e:
        raise e
    finally:
        if cluster:
            cluster.close()


if __name__ == '__main__':
    main()
