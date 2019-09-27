import os
from smart_open import open as s_open
import boto3
from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT
from impresso_db.base import engine


def take_mysql_snapshot():
    """Fetch the list of content item IDs from MySQL."""

    db_config = os.environ["IMPRESSO_DB_CONFIG"]
    print(f'Connecting to MySQL DB {db_config}')

    with engine.connect() as db_conn:
        q = "SELECT id FROM content_items;"
        mysql_ids = db_conn.execute(q)

    ci_ids = [
        db_id
        for db_id in mysql_ids
    ]
    print(f'Fetched {len(ci_ids)} content item IDs from DB')
    return ci_ids


def take_canonical_snapshot(input_bucket: str):
    """Fetch the list of content item IDs from S3."""

    # fetch the list of newspapers from the DB
    # use glob to get the list of issue files from s3
    # return the list of content item ids
    pass


# TODO: make it work also with local directory not only s3
def write_snapshot_to_s3(data: list, path: str):

    session = boto3.Session(
        aws_access_key_id=IMPRESSO_STORAGEOPT['key'],
        aws_secret_access_key=IMPRESSO_STORAGEOPT['secret'],
    )
    s3_endpoint = IMPRESSO_STORAGEOPT['client_kwargs']['endpoint_url']

    transport_params = {
        'session': session,
        'resource_kwargs': {
            'endpoint_url': s3_endpoint,
        }
    }

    with s_open(path, 'w', transport_params=transport_params) as outfile:
        outfile.write("\n".join(data))
    return


mysql_ci_ids = take_mysql_snapshot()
print(len(mysql_ci_ids))
