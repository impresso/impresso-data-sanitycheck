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
from typing import Dict

from impresso_commons.utils.kube import (
    make_scheduler_configuration,
    make_worker_configuration,
)
from sanity_check.contents.s3_data import fetch_issues, fetch_page_ids, fetch_issue_ids
import pandas as pd
from dask import bag
import os

OUTPUT_SEPARATOR = "\n#####"


def check_duplicated_content_item_IDs(issue_bag: bag.Bag) -> pd.DataFrame:
    """Short summary.

    ..note::
        This is a global check.

    :param bag.Bag issue_bag: Description of parameter `issue_bag`.
    :return: Description of returned object.
    :rtype: pd.DataFrame

    """
    duplicates = (
        issue_bag.map(lambda issue_json: [ci["m"]["id"] for ci in issue_json["i"]])
        .flatten()
        .frequencies()
        .filter(lambda i: i[1] > 1)
        .map(lambda i: {"ci_id": i[0], "freq": i[1], "newspaper_id": i[0].split("-")[0]})
        .compute()
    )

    if duplicates:
        duplicates_df = pd.DataFrame(duplicates).set_index("ci_id")
    else:
        # there are no duplicates
        duplicates_df = pd.DataFrame(columns=["ci_id", "freq", "newspaper_id"])

    print(
        (
            f"Found {duplicates_df.shape[0]} duplicated "
            "content item IDs, belonging to "
            f"{duplicates_df.newspaper_id.unique().size} journals"
            f"({', '.join(list(duplicates_df.newspaper_id.unique()))})"
        )
    )
    return duplicates_df


def check_duplicated_issues_IDs(issue_bag: bag.Bag) -> pd.DataFrame:
    """Check that newspaper issue IDs are unique within the corpus."""

    duplicate_issue_ids = (
        issue_bag.pluck("id")
        .frequencies()
        .filter(lambda i: i[1] > 1)
        .map(
            lambda i: {
                "issue_id": i[0],
                "freq": i[1],
                "newspaper_id": i[0].split("-")[0],
            }
        )
        .compute()
    )
    print(f"{len(duplicate_issue_ids)} duplicated IDs were found")
    return pd.DataFrame(duplicate_issue_ids).set_index("issue_id")


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
        page_ids_from_issues.map(lambda id: {"id": id, "from_issues": True}).to_dataframe().set_index("id").persist()
    )

    df_page_ids_from_pages = (
        page_ids_from_pages.map(lambda id: {"id": id, "from_pages": True}).to_dataframe().set_index("id").persist()
    )

    df_pages = df_page_ids_from_issues.join(df_page_ids_from_pages, how="outer").compute()

    df_pages["newspaper_id"] = df_pages.index.map(lambda z: z.split("-")[0])
    return df_pages[~(df_pages.from_pages == df_pages.from_issues)]


def run_checks_canonical(canonical_bucket_name, output_dir=None):
    canonical_issues_bag = fetch_issues(canonical_bucket_name, compute=False).filter(lambda i: len(i) > 0)

    # 1) verify that there are not duplicated issue IDs
    print(OUTPUT_SEPARATOR)
    print('Verifying existence of duplicated issue IDs...')
    duplicated_issues_df = check_duplicated_issues_IDs(canonical_issues_bag)
    print('Done')
    if output_dir and os.path.exists(output_dir):
        fname = "duplicate_issue_ids"
        pickle_path = os.path.join(output_dir, f"{fname}.pkl")
        csv_path = os.path.join(output_dir, f"{fname}.csv")

        duplicated_issues_df.to_pickle(pickle_path)
        print(f"Written pickle file to {pickle_path}")

        duplicated_issues_df.to_csv(csv_path)
        print(f"Written CSV output to {csv_path}")
    elif os.path.exists(output_dir) is False:
        print(f"No outputs written as folder {output_dir} does not exist.")

    # 2) verify that there are not duplicated content item IDs
    print(OUTPUT_SEPARATOR)
    print('Verifying existence of duplicated content item IDs...')
    duplicates_df = check_duplicated_content_item_IDs(canonical_issues_bag)
    if output_dir and os.path.exists(output_dir):
        fname = "duplicate_ci_ids"
        pickle_path = os.path.join(output_dir, f"{fname}.pkl")
        csv_path = os.path.join(output_dir, f"{fname}.csv")

        duplicates_df.to_pickle(pickle_path)
        print(f"Written pickle file to {pickle_path}")

        duplicates_df.to_csv(csv_path)
        print(f"Written CSV output to {csv_path}")
    elif os.path.exists(output_dir) is False:
        print(f"No outputs written as folder {output_dir} does not exist.")
    print('Done')

    # 3) verify the consistency of page IDs
    print(OUTPUT_SEPARATOR)
    print('Verifying integrity of page IDs...')
    inconsistencies_df = check_inconsistent_page_ids(canonical_bucket_name)
    if output_dir and os.path.exists(output_dir):
        fname = "inconsistent_page_ids"
        pickle_path = os.path.join(output_dir, f"{fname}.pkl")
        csv_path = os.path.join(output_dir, f"{fname}.csv")

        inconsistencies_df.to_pickle(pickle_path)
        print(f"Written pickle file to {pickle_path}")

        inconsistencies_df.to_csv(csv_path)
        print(f"Written CSV output to {csv_path}")
    elif os.path.exists(output_dir) is False:
        print(f"No outputs written as folder {output_dir} does not exist.")
    print('Done')

    # TODO: at this point output a list of newspaper that can be moved
    # to staging
    return


def main():
    arguments = docopt(__doc__)
    s3_canonical_bucket = arguments["--canonical-bucket"]
    s3_rebuilt_bucket = arguments["--rebuilt-bucket"]
    output_dir = arguments["--output-dir"]
    memory = arguments["--k8-memory"] if arguments["--k8-memory"] else "1G"
    workers = int(arguments["--k8-workers"]) if arguments["--k8-workers"] else 50

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

        # NB here we check that scheduler and workers do have the same
        # versions of the various libraries, as mismatches may cause
        # exceptions and weird behaviours.
        libraries_versions = dask_client.get_versions(check=True)
        print(dask_client)
        run_checks_canonical(s3_canonical_bucket, output_dir)

        # TODO: do stuff

    except Exception as e:
        raise e
    finally:
        if cluster:
            cluster.close()


if __name__ == "__main__":
    main()
