from .s3_data import fetch_issue_ids, fetch_issue_ids_rebuilt


def check_sync_db():
    """
    Check which canonical issues are not present in the DB.

    Return a dataframe with detailed information.
    """

    # get list of issue IDs from s3
    s3_canonical_issues = fetch_issue_ids(compute=False)
    print(len(s3_canonical_issues))

    # TODO: do the same for MySQL


def check_sync_rebuilt():
    """
    Check which canonical issues have not been rebuilt.

    Return a dataframe with detailed information.
    """

    s3_canonical_issues = fetch_issue_ids(compute=False)
    s3_rebuilt_issues = fetch_issue_ids_rebuilt(compute=False)
    assert s3_rebuilt_issues and s3_canonical_issues


"""
Other functions:
- generate a JSON configuration file starting from output of `check_sync_db`
- generate a JSON configuration file starting from output of `check_sync_rebuilt`

All this eventually should be called with a CLI.
""" # noqa
