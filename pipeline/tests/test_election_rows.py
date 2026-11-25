import pandas as pd

from frt.scraping.polling import process_hierarchical_columns, row_is_election_result


def _table():
    link = "https://en.wikipedia.org/wiki/AUR_Alliance"
    columns = ["Date", "Poll source", "Sample size", ("AUR", link)]
    df = pd.DataFrame(
        [
            ["3-8 Jan 2024", "INSCOP", "1,000", "13.0"],
            ["9 June 2024", "2024 European election", "N/a", "14.93"],
            ["9 June 2024", "2024 Local Election", "N/a", "9.61"],
        ],
        columns=columns[:3] + ["AUR"],
    )
    cols_info = process_hierarchical_columns(df)
    for col_info in cols_info:
        if col_info["column_name"] == "AUR":
            col_info["column_link"] = link
    return df, cols_info


def test_election_row_is_found_when_source_is_not_the_first_column():
    df, cols_info = _table()
    assert [row_is_election_result(df.iloc[i], cols_info) for i in range(3)] == [False, True, True]
