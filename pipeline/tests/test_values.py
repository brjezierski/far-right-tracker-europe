import pytest

from frt.scraping.values import extract_percentage_value, get_polling_value


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("23.5%", 23.5), ("23,5", 23.5), ("23.5 (note)", 23.5), (" 7% ", 7.0)],
)
def test_extract_percentage_value(raw, expected):
    assert extract_percentage_value(raw) == expected


def test_get_polling_value_dash_is_missing():
    assert get_polling_value("-") is None


def test_get_polling_value_number():
    assert get_polling_value("17.2%") == 17.2
