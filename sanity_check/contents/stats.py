"""Command-line script to generate stats about impresso corpus/data.

Usage:
    stats.py s3 --input-bucket=<ib> --output-dir=<od> [--output-bucket=<ob> --k8-memory=<mem> --k8-workers=<wkrs> --flat]
    stats.py mysql --db-config=<dbcfg> --output-dir=<od>
    stats.py corpus --canonical-bucket=<cb> --rebuilt-bucket=<rb> --db-config=<db> --output-dir=<od> --output-bucket=<ob> [--k8-memory=<mem> --k8-workers=<wkrs>]

Options:

--input-bucket=<ib>  TODO

Example:

    python stats.py
"""  # noqa: E501

import os
import json
import ipdb  # TODO remove later on
import pandas as pd
from dask_k8 import DaskCluster
from dask import bag as db
from docopt import docopt
from pathlib import Path
import tabulate

from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT, fixed_s3fs_glob
from impresso_commons.utils.s3 import alternative_read_text
from impresso_commons.utils.kube import (
    make_scheduler_configuration,
    make_worker_configuration,
)
from sanity_check.contents.mysql import list_issues as mysql_list_issues
from sanity_check.contents.s3_data import fetch_issue_ids, fetch_issue_ids_rebuilt, fetch_issues


def fetch_newspapers_metadata(db_config: str = None) -> pd.DataFrame:
    """Fetches from DB basic metadata about newspapers needed for the stats.

    :param str db_config: DB configuration to use (e.g. "dev", "prod", etc.).
    :return: A pandas DataFrame containing the metadata, with the newspaper ID as its index.
    :rtype: pd.DataFrame

    """
    if db_config:
        os.environ["IMPRESSO_DB_CONFIG"] = db_config

    config = os.environ["IMPRESSO_DB_CONFIG"]
    print(f'Connecting to MySQL DB {config}')

    # it needs to be imported here (not earlier!) otherwise we can't change/overwrite the DB config
    from impresso_db.base import engine

    with engine.connect() as db_conn:
        q = "SELECT id,title, start_year, end_year FROM newspapers;"
        mysql_data = db_conn.execute(q)

    np_ids = [{"id": datum[0], "title": datum[1], "start_year": datum[2], "end_year": datum[3]} for datum in mysql_data]
    print(f'Fetched {len(np_ids)} newspaper IDs from DB')

    df = pd.DataFrame(np_ids).set_index('id')
    df = df[pd.notnull(df.start_year) & pd.notnull(df.end_year)]
    df.start_year = df.start_year.astype(int)
    df.end_year = df.end_year.astype(int)

    return df


def fetch_access_rights(s3_canonical_bucket: str, output_dir: str) -> pd.DataFrame:
    """Fetches license information (access rights) per newspaper issue.

    :param str s3_canonical_bucket: S3 bucket with canonical data.
    :param str output_dir: Directory where to store intermediate pickle.
    :return: A pandas DataFrame withe issue ID as the index and a `license` column.
    :rtype: pd.DataFrame

    """

    issue_bag = fetch_issues(s3_canonical_bucket, compute=False)

    license_df = (
        issue_bag.map(lambda i: {"issue_id": i['id'], "license": i['ar']})
        .to_dataframe()
        .set_index('issue_id')
        .persist()
    )

    pd_license_df = license_df.compute()
    pd_license_df['year'] = pd_license_df.index.map(lambda idx: idx.split('-')[1])
    pd_license_df.license.replace({'open_private': 'closed'}, inplace=True)
    license_by_year = pd_license_df.pivot_table(index='year', columns='license', values='license', aggfunc=len)

    Path(output_dir).mkdir(exist_ok=True)
    license_by_year.to_pickle(os.path.join(output_dir, 'issue_license_by_year.pkl'))

    return license_df


def compute_canonical_stats(s3_canonical_bucket: str) -> pd.DataFrame:
    """Computes number of issues and pages per newspaper from canonical data in s3.

    :param str s3_canonical_bucket: S3 bucket with canonical data.
    :return: A pandas DataFrame with newspaper ID as the index and columns `n_issues`, `n_pages`.
    :rtype: pd.DataFrame

    """

    s3_canonical_issues = fetch_issues(s3_canonical_bucket, compute=False)

    pages_count_df = (
        s3_canonical_issues.map(
            lambda i: {"np_id": i["id"].split('-')[0], "id": i['id'], "issue_id": i['id'], "n_pages": len(set(i['pp']))}
        )
        .to_dataframe(meta={'np_id': str, 'id': str, 'issue_id': str, "n_pages": int})
        .set_index('id')
        .persist()
    )

    # create dataframe with number of pages by newspaper
    df = pages_count_df.groupby(by='np_id').agg({"n_pages": sum}).compute()

    # add to the dataframe the number of issues by newspaper
    issue_count_by_np = pages_count_df.groupby(by='np_id').size().compute()
    df['n_issues'] = issue_count_by_np

    return df


def compute_rebuilt_stats(s3_rebuilt_bucket: str, s3_canonical_bucket: str, output_dir: str) -> pd.DataFrame:
    """Computes number of tokens and images per newspaper from rebuilt data in s3.

    ..note::

        In the process, it also fetches license information per issue and adds it to each content item.
        This intermediate dask dataframe is then serialized for future (re-)use to s3 as per `s3_canonical_bucket`

    :param str s3_rebuilt_bucket: S3 bucket with rebuilt data.
    :param str s3_canonical_bucket: S3 bucket with canonical data.
    :param str output_dir: Description of parameter `output_dir`.
    :return: A pandas DataFrame with newspaper ID as the index and columns `n_tokens`, `n_images`.
    :rtype: pd.DataFrame

    """

    rebuilt_files = fixed_s3fs_glob(f'{s3_rebuilt_bucket}/*.bz2')
    print(f"Found {len(rebuilt_files)} files")

    rebuilt_data = (
        db.from_sequence(rebuilt_files, partition_size=10)
        .map(alternative_read_text, IMPRESSO_STORAGEOPT)
        .flatten()
        .map(json.loads)
        .map(
            lambda i: {
                "id": i['id'],
                "year": i['id'].split('-')[1],
                "newspaper": i['id'].split('-')[0],
                "issue_id": "-".join(i['id'].split("-")[:-1]),
                "type": i['tp'],
                "n_tokens": len(i['ft'].split()) if "ft" in i else None,
                "title_length": len(i['t']) if "t" in i and i['t'] else None,
            }
        )
        .persist()
    )

    rebuilt_df = (
        rebuilt_data.to_dataframe(
            meta={
                "id": str,
                "year": str,
                "newspaper": str,
                "issue_id": str,
                "type": str,
                "n_tokens": object,
                "title_length": object,
            }
        )
        .set_index('id')
        .persist()
    )

    license_df = fetch_access_rights(s3_canonical_bucket, output_dir)
    rebuilt_enriched_df = rebuilt_df.join(license_df, on='issue_id').persist()
    # files = enriched_df.to_csv(
    #    "s3://impresso-stats/contentitem-license-stats/",
    #    storage_options=IMPRESSO_STORAGEOPT
    # )

    # calculate the number of tokens per newspaper and add it to the dataframe
    df = rebuilt_df.groupby(by='newspaper').agg({"n_tokens": sum}).compute()
    df.n_tokens = df.n_tokens.astype('int')

    # breakdown of content items by type
    counts_by_type = rebuilt_df.groupby(by='type').size().compute()
    print(f"Total number of content items: {rebuilt_df.shape[0].compute()}")
    print(counts_by_type)  # use tabulate here

    # calculate the number of images per newspaper and add it to the dataframe
    imgs_by_nps = rebuilt_df[rebuilt_df.type == 'img'].groupby(by=['newspaper']).size().compute()
    imgs_by_nps_df = pd.DataFrame({"n_images": imgs_by_nps})
    df = df.join(imgs_by_nps_df, how='outer')
    df.fillna(0, inplace=True)
    df.n_images = df.n_images.astype(int)

    # files = evenized_light_df.to_csv("s3://impresso-stats/content-item-stats/", storage_options=IMPRESSO_STORAGEOPT)
    return df


def compute_corpus_stats(s3_canonical_bucket: str, s3_rebuilt_bucket: str, db_config: str, output_dir: str) -> None:
    """Computes corpus statistics from data in MySQL DB as well as in S3.

    :param str s3_canonical_bucket: S3 bucket with canonical data.
    :param str s3_rebuilt_bucket: S3 bucket with rebuilt data.
    :param str db_config: DB configuration to use (e.g. "dev", "prod", etc.).
    :param str output_dir: Description of parameter `output_dir`.
    :return: Description of returned object.
    :rtype: None

    """
    stats_df = fetch_newspapers_metadata(db_config)
    canonical_stats_df = compute_canonical_stats(s3_canonical_bucket)
    rebuilt_stats_df = compute_rebuilt_stats(s3_rebuilt_bucket, s3_canonical_bucket, output_dir)

    # do various joins
    corpus_stats_df = stats_df.join(canonical_stats_df, how='inner')
    corpus_stats_df = corpus_stats_df.join(rebuilt_stats_df, how='inner')
    corpus_stats_df = corpus_stats_df[
        ["title", "start_year", "end_year", "n_issues", "n_pages", "n_tokens", "n_images"]
    ]

    # impresso rundown:
    print(f"number of newspaper issues {corpus_stats_df.n_issues.sum()}")
    print(f"number of newspaper pages {corpus_stats_df.n_pages.sum()}")
    print(f"number of words {corpus_stats_df.n_tokens.sum()}")
    print(f"number of images {corpus_stats_df.n_images.sum()}")

    # serialize table to markdown
    serialize_markdown_table(corpus_stats_df, output_dir)

    # serialise to CSV
    corpus_stats_df.to_csv(os.path.join(output_dir, 'newspaper_stats.csv'))


def serialize_markdown_table(corpus_stats_df: pd.DataFrame, output_dir: str) -> None:
    hs = [
        'newspaper id',
        'newspaper title',
        'start year',
        'end year',
        'n. issues',
        'n. pages',
        'n. tokens',
        'n. images',
    ]

    print("\nHere a preview of the corpus stats:")
    print(tabulate.tabulate(corpus_stats_df, tablefmt='grid', headers=hs))

    markdown_data = tabulate.tabulate(corpus_stats_df, tablefmt='pipe', headers=hs)

    with open(os.path.join(output_dir, 'corpus_stats_table.md'), 'w', 'utf-8') as outfile:
        outfile.write(markdown_data)


def main():
    arguments = docopt(__doc__)
    s3_stats = arguments['s3']
    db_stats = arguments['mysql']
    corpus_stats = arguments['corpus']
    s3_canonical_bucket = arguments['--canonical-bucket']
    s3_rebuilt_bucket = arguments['--rebuilt-bucket']
    s3_input_bucket = arguments['--input-bucket']
    s3_output_bucket = arguments['--output-bucket']
    output_dir = arguments['--output-dir']
    db_config = arguments['--db-config']
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

        if s3_stats:
            print("Not implemented yet!")
        elif db_stats:
            print("Not implemented yet!")
        elif corpus_stats:
            compute_corpus_stats(s3_canonical_bucket, s3_rebuilt_bucket, db_config, output_dir)

    except Exception as e:
        raise e
    finally:
        if cluster:
            cluster.close()


if __name__ == '__main__':
    main()
