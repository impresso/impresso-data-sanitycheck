"""Command-line script to generated configuration files for ingestion/rebuild scripts.

Usage:
    sync.py s3 --canonical-bucket=<cb> --rebuilt-bucket=<rb> --output-dir=<od> [--k8-memory=<mem> --k8-workers=<wkrs>]
    sync.py db --canonical-bucket=<cb> --db-config=<db> --output-dir=<od> [--k8-memory=<mem> --k8-workers=<wkrs>]

Options:

--canonical-bucket=<cb>  S3 bucket where canonical JSON data will be read from

Example:

    python sanity_check/contents/sync.py s3 --canonical-bucket='s3://original-canonical-staging' \
    --rebuilt-bucket='s3://passim-rebuilt' --output-dir=./ --k8-memory="1G" --k8-workers=25
"""  # noqa: E501

import os
import json
import pandas as pd
from dask_k8 import DaskCluster
from docopt import docopt

from impresso_commons.utils.kube import (
    make_scheduler_configuration,
    make_worker_configuration,
)
from sanity_check.contents.mysql import list_issues as mysql_list_issues
from sanity_check.contents.s3_data import (
    fetch_issue_ids,
    fetch_issue_ids_rebuilt,
)


def sync_db(s3_bucket_name: str, mysql_db_config: str) -> pd.DataFrame:
    """
    Check which canonical issues from S3 are not present in the DB.

    # TODO: serialise dataframe somewhere

    Return a dataframe with detailed information.
    """

    # get list of issue IDs from s3
    s3_issues_ids = fetch_issue_ids(s3_bucket_name)

    # do the same for MySQL
    mysql_issue_ids = mysql_list_issues(mysql_db_config)

    # create dataframe with issue ids from s3
    s3_issue_data = pd.DataFrame(
        [{"id": issue_id, "in_s3": True} for issue_id in s3_issues_ids]
    ).set_index('id')
    # create dataframe with issue ids from MySQL
    mysql_issue_data = pd.DataFrame(
        [{"id": issue_id, "in_mysql": True} for issue_id in mysql_issue_ids]
    ).set_index('id')

    # we combine the two sets of IDs with an outer join
    issue_data = mysql_issue_data.join(s3_issue_data, how='outer')

    # and return only those that are in S3 but not in MySQL
    issues_to_ingest = issue_data[
        (issue_data.in_mysql.isnull()) & issue_data.in_s3.notnull()
    ]
    issues_to_ingest['newspaper_id'] = issues_to_ingest.index.map(
        lambda i: i.split('-')[0]
    )
    issues_to_ingest['year'] = issues_to_ingest.index.map(
        lambda i: int(i.split('-')[1])
    )

    print(
        (
            f'There are {issues_to_ingest.shape[0]} issues from '
            f'{s3_bucket_name} missing from MySQL ({mysql_db_config}).'
        )
    )

    return issues_to_ingest


def sync_rebuilt(canonical_bucket_name: str, rebuilt_bucket_name: str) -> tuple:
    """
    Check which canonical issues have not been rebuilt, and which rebuilt
    data are not yet ingested into canonical.

    # TODO: serialise dataframes somewhere

    Return a dataframe with detailed information.
    """

    s3_canonical_issues = fetch_issue_ids(canonical_bucket_name, compute=False)
    s3_rebuilt_issues = fetch_issue_ids_rebuilt(
        rebuilt_bucket_name, compute=False
    )

    # the s3 rebuilt bucket is not empty
    if s3_rebuilt_issues:

        s3_rebuilt_data = (
            s3_rebuilt_issues.map(lambda i: {'id': i, "in_rebuilt": True})
            .to_dataframe()
            .set_index('id')
            .compute()
        )

        s3_canonical_data = (
            s3_canonical_issues.map(lambda i: {'id': i, "in_canonical": True})
            .to_dataframe()
            .set_index('id')
            .compute()
        )

        print('Joining the two dataframes')
        issue_data = s3_rebuilt_data.join(s3_canonical_data, how='outer')
        print('Joining the two dataframes... done')
    # the s3 rebuilt bucket is empty!
    else:
        issue_data = (
            s3_canonical_issues.map(
                lambda i: {'id': i, "in_canonical": True, "in_rebuilt": None}
            )
            .to_dataframe()
            .set_index('id')
            .compute()
        )

    issue_data['newspaper_id'] = issue_data.index.map(lambda i: i.split('-')[0])
    issue_data['year'] = issue_data.index.map(lambda i: int(i.split('-')[1]))

    issues_to_rebuild = issue_data[
        (issue_data.in_canonical.notnull()) & (issue_data.in_rebuilt.isnull())
    ]
    issues_to_ingest = issue_data[
        (issue_data.in_canonical.isnull()) & (issue_data.in_rebuilt.notnull())
    ]

    return (issues_to_ingest, issues_to_rebuild)


def configure_db_ingestion(
    s3_bucket_name: str, mysql_db_config: str, issues_to_ingest: pd.DataFrame
) -> list:
    """Generate the config file for DB ingestion in a data-driven fashion."""
    config = []

    if issues_to_ingest is None:
        issues_to_ingest = sync_db(s3_bucket_name, mysql_db_config)

    for key, group in issues_to_ingest.groupby(by='newspaper_id'):
        missing_years = sorted(set(group.year))
        config.append({key: [min(missing_years), max(missing_years) + 1]})
    return config


def configure_rebuild(
    canonical_bucket_name: str,
    rebuilt_bucket_name: str,
    issues_to_rebuild: pd.DataFrame = None,
) -> list:
    """Generate the config file for data rebuild in a data-driven fashion.

    The configuration file is generated by comparing the newspaper issue IDs
    contained in the s3 canonical bucket against those in the s3 rebuilt
    bucket.
    """
    config = []

    if issues_to_rebuild is None:
        issues_to_ingest, issues_to_rebuild = sync_rebuilt(
            canonical_bucket_name, rebuilt_bucket_name
        )

    print(
        (
            f'Watch out: there are {issues_to_rebuild.shape[0]} issues '
            f'that are already ingested ({canonical_bucket_name}) but not '
            f'yet rebuilt ({rebuilt_bucket_name}).'
        )
    )

    for key, group in issues_to_rebuild.groupby(by='newspaper_id'):
        missing_years = sorted(set(group.year))
        config.append({key: [min(missing_years), max(missing_years) + 1]})
    return config


def configure_ingestion(
    canonical_bucket_name: str,
    rebuilt_bucket_name: str,
    issues_to_ingest: pd.DataFrame = None,
) -> list:
    """Generate the config file for data rebuild in a data-driven fashion.

    The configuration file is generated by comparing the newspaper issue IDs
    contained in the s3 canonical bucket against those in the s3 rebuilt
    bucket.
    """
    config = []

    if issues_to_ingest is None:
        issues_to_ingest, issues_to_rebuild = sync_rebuilt(
            canonical_bucket_name, rebuilt_bucket_name
        )

    print(
        (
            f'Watch out: there are {issues_to_ingest.shape[0]} issues '
            f'that are already rebuilt ({rebuilt_bucket_name}) but not '
            f'yet ingested ({canonical_bucket_name}).'
        )
    )

    for key, group in issues_to_ingest.groupby(by='newspaper_id'):
        missing_years = sorted(set(group.year))
        config.append({key: [min(missing_years), max(missing_years) + 1]})
    return config


def run_s3_sync(
    canonical_bucket_name: str, rebuilt_bucket_name: str, output_dir: str
) -> None:
    """Short summary.

    :param str canonical_bucket_name: Name of S3 bucket with canonical data.
    :param str rebuilt_bucket_name: Name of S3 bucket with rebuilt data.
    :param str output_dir: Description of parameter `output_dir`.
    :param str k8_memory: Memory to be given each worker in the dask kubernetes
        cluster (e.g. "10G").
    :param int k8_workers_n: Numnber of workers to create when initialising
        the dask kubernetes cluster.
    :return: None
    :rtype: None

    """
    try:

        issues_to_ingest, issues_to_rebuild = sync_rebuilt(
            canonical_bucket_name, rebuilt_bucket_name
        )

        # serialize dataframes for later
        issues_to_ingest.to_pickle(
            os.path.join(output_dir, 'issues_to_ingest.pkl')
        )
        issues_to_rebuild.to_pickle(
            os.path.join(output_dir, 'issues_to_rebuild.pkl')
        )

        ingestion_config = configure_ingestion(
            canonical_bucket_name,
            rebuilt_bucket_name,
            issues_to_ingest=issues_to_ingest,
        )

        rebuild_config = configure_rebuild(
            canonical_bucket_name,
            rebuilt_bucket_name,
            issues_to_rebuild=issues_to_rebuild,
        )

        # write the generated configurations to a file
        rebuild_cfg_path = os.path.join(output_dir, 'rebuild-config.json')
        with open(rebuild_cfg_path, 'w') as cfg_file:
            json.dump(rebuild_config, cfg_file, indent=4)

        ingestion_cfg_path = os.path.join(output_dir, 'ingestion-config.json')
        with open(ingestion_cfg_path, 'w') as cfg_file:
            json.dump(ingestion_config, cfg_file, indent=4)

    except Exception as e:
        raise e


def run_db_sync(
    canonical_bucket_name: str, db_config: str, output_dir: str
) -> None:

    issues_to_ingest = sync_db(canonical_bucket_name, db_config)

    issues_to_ingest.to_pickle(
        os.path.join(output_dir, 'issues_to_ingest_db.pkl')
    )

    dbingestion_config = configure_db_ingestion(
        canonical_bucket_name, db_config, issues_to_ingest
    )

    # write the generated configurations to a file
    dbingestion_cfg_path = os.path.join(output_dir, 'dbingest-config.json')
    with open(dbingestion_cfg_path, 'w') as cfg_file:
        json.dump(dbingestion_config, cfg_file, indent=2)


def main():
    arguments = docopt(__doc__)
    s3_sync = arguments['s3']
    db_sync = arguments['db']
    s3_canonical_bucket = arguments['--canonical-bucket']
    s3_rebuilt_bucket = arguments['--rebuilt-bucket']
    output_dir = arguments['--output-dir']
    db_config = arguments['--db-config']
    memory = arguments['--k8-memory'] if arguments['--k8-memory'] else "1G"
    workers = (
        int(arguments['--k8-workers']) if arguments['--k8-workers'] else 50
    )

    image_uri = "ic-registry.epfl.ch/dhlab/impresso_data-sanity-check:v1"

    try:
        # first thing to do is to create the dask kubernetes cluster
        cluster = DaskCluster(
            namespace="dhlab",
            cluster_id="impresso-sanitycheck-cli",
            scheduler_pod_spec=make_scheduler_configuration(),
            worker_pod_spec=make_worker_configuration(
                docker_image=image_uri, memory=memory
            ),
        )
        cluster.create()
        cluster.scale(workers, blocking=True)
        dask_client = cluster.make_dask_client()
        dask_client.get_versions(check=True)
        print(dask_client)

        if s3_sync:
            run_s3_sync(
                canonical_bucket_name=s3_canonical_bucket,
                rebuilt_bucket_name=s3_rebuilt_bucket,
                output_dir=output_dir,
            )
        elif db_sync:
            run_db_sync(
                canonical_bucket_name=s3_canonical_bucket,
                db_config=db_config,
                output_dir=output_dir,
            )

    except Exception as e:
        raise e
    finally:
        if cluster:
            cluster.close()


if __name__ == '__main__':
    main()
