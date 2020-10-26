"""Functions to fetch impresso data from MySQL DB."""

import os


def list_newspapers(db_config=None):
    if db_config:
        os.environ["IMPRESSO_DB_CONFIG"] = db_config

    config = os.environ["IMPRESSO_DB_CONFIG"]
    print(f'Connecting to MySQL DB {config}')

    #
    from impresso_db.base import engine

    with engine.connect() as db_conn:
        q = "SELECT newspaper_id FROM np_timespan_v;"
        mysql_ids = db_conn.execute(q)

    np_ids = [db_id[0] for db_id in mysql_ids]
    print(f'Fetched {len(np_ids)} content item IDs from DB')
    return np_ids


def list_issues(db_config=None):
    if db_config:
        os.environ["IMPRESSO_DB_CONFIG"] = db_config

    config = os.environ["IMPRESSO_DB_CONFIG"]
    print(f'Connecting to MySQL DB {config}')

    #
    from impresso_db.base import engine

    with engine.connect() as db_conn:
        q = "SELECT id FROM issues;"
        mysql_ids = db_conn.execute(q)

    issue_ids = [db_id[0] for db_id in mysql_ids]
    print(f'Fetched {len(issue_ids)} issue IDs from DB')
    return issue_ids


def list_content_items(db_config=None):
    if db_config:
        os.environ["IMPRESSO_DB_CONFIG"] = db_config

    config = os.environ["IMPRESSO_DB_CONFIG"]
    print(f'Connecting to MySQL DB {config}')

    #
    from impresso_db.base import engine

    with engine.connect() as db_conn:
        print(f'Fetching content item IDs from MySQL DB {config}')
        q = "SELECT id FROM content_items;"
        mysql_ids = db_conn.execute(q)

    ci_ids = [db_id[0] for db_id in mysql_ids]
    print(f'Fetched {len(ci_ids)} IDs.')
    return ci_ids
