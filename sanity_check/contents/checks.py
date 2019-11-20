from collections import Counter
from .s3_data import fetch_issues, fetch_page_ids, fetch_issue_ids
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
    duplicates_bag = issue_bag.map(
        find_duplicated_content_item_IDs
    ).flatten()

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
        duplicates_df = pd.DataFrame(
            columns=['id', 'issue_id', 'newspaper_id', 'year']
        )

    print((
        f'Found {duplicates_df.shape[0]} duplicated '
        'content item IDs, belonging to '
        f'{duplicates_df.newspaper_id.unique().size} journals.'
    ))
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
def check_inconsistent_page_ids(
    canonical_bucket_name: str
) -> pd.DataFrame:
    """Check whether there are mismatches between page IDs.


    Page IDs are found in two places in our data:
    1. field `pp` of issue JSON
    2. field `id` of page JSON

    As there can be mismatches between these two sets of identifiers,
    we need to verify that all page IDs in #1 are contained within #2.
    """

    page_ids_from_issues = fetch_page_ids(
        canonical_bucket_name,
        source="issues"
    )
    page_ids_from_pages = fetch_page_ids(
        canonical_bucket_name,
        source="pages"
    )

    df_page_ids_from_issues = bag.from_sequence(set(page_ids_from_issues)).map(
        lambda id: {"id": id, "from_issues": True}
    ).to_dataframe().set_index('id').persist()

    df_page_ids_from_pages = bag.from_sequence(page_ids_from_pages).map(
        lambda id: {"id": id, "from_pages": True}
    ).to_dataframe().set_index('id').persist()

    df_pages = df_page_ids_from_issues.join(
        df_page_ids_from_pages,
        how='outer'
    ).compute()

    df_pages['newspaper_id'] = df_pages.index.map(lambda z: z.split('-')[0])
    return df_pages[~(df_pages.from_pages == df_pages.from_issues)]


def run_checks_canonical(canonical_bucket_name, output_dir=None):
    canonical_issues_bag = fetch_issues(
        canonical_bucket_name,
        compute=False
    ).filter(lambda i: len(i) > 0)

    # 1) verify that there are not duplicated content item IDs
    duplicates_df = check_duplicated_content_item_IDs(canonical_issues_bag)

    if output_dir and os.path.exists(output_dir):
        fname = "duplicate_ci_ids"
        duplicates_df.to_pickle(os.path.join(output_dir, f"{fname}.pkl"))
        duplicates_df.to_csv(os.path.join(output_dir, f"{fname}.csv"))

    # 2) verify the consistency of page IDs
    pages_df = check_inconsistent_page_ids(canonical_issues_bag)
    print(pages_df.head())

    return duplicates_df
