import pandas as pd

from frt.scraping.polling import find_pollster_column, process_hierarchical_columns
from frt.scraping.sources import COUNTRY_EXCLUDED_HEADERS
from frt.scraping.tables import find_date_column

WIKI = "https://en.wikipedia.org/wiki/"


def _cols(columns):
    df = pd.DataFrame([[""] * len(columns)], columns=columns)
    return process_hierarchical_columns(df)


def test_footnote_anchor_is_not_a_party_link():
    columns = [
        "Date",
        (("SF", WIKI + "Sinn_F%C3%A9in"), ()),
        (("O/I [ nb 1 ]", "#cite_note-remainder-3"), ()),
    ]
    cols_info = _cols(columns)
    assert [c["column_link"] for c in cols_info] == ["", WIKI + "Sinn_F%C3%A9in", ""]


def test_pollster_is_not_the_date_column_when_date_comes_first():
    cols_info = _cols(["Last date of polling", "Commissioner", "Polling firm", "Sample size"])
    date_col = find_date_column(cols_info)
    assert find_pollster_column(cols_info, date_col) == "Polling firm"


def test_pollster_falls_back_to_next_column_after_date():
    cols_info = _cols(["Date", "Source", "Sample size"])
    assert find_pollster_column(cols_info, find_date_column(cols_info)) == "Source"


def test_pollster_stays_first_column_when_date_is_elsewhere():
    cols_info = _cols(["Polling firm", "Fieldwork date", "Sample size"])
    assert find_pollster_column(cols_info, find_date_column(cols_info)) == "Polling firm"


def test_ireland_skips_leader_preference_table():
    assert "Preferred Taoiseach" in COUNTRY_EXCLUDED_HEADERS["Ireland"]
