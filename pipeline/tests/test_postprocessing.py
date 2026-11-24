import pandas as pd

from frt.postprocessing import (
    calculate_rolling_average,
    create_daily_series,
    drop_duplicate_polls,
    filter_pre_2010_datapoints,
    remove_isolated_datapoints,
)


def _df(rows):
    df = pd.DataFrame(rows, columns=["date", "party", "polling_value"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def test_filter_pre_2010_keeps_cutoff_year_onwards():
    df = _df([("2009-12-31", "A", 1.0), ("2010-01-01", "A", 2.0), ("2020-05-05", "A", 3.0)])
    out = filter_pre_2010_datapoints(df, cutoff_year=2010)
    assert list(out["polling_value"]) == [2.0, 3.0]


def test_remove_isolated_datapoints_drops_lone_point():
    rows = [(f"2020-01-{d:02d}", "A", 10.0) for d in range(1, 6)]
    rows.append(("2023-06-01", "A", 40.0))  # more than a year from every other point
    out = remove_isolated_datapoints(_df(rows), min_neighbors=2)
    assert 40.0 not in set(out["polling_value"])
    assert len(out) == 5


def test_calculate_rolling_average_needs_full_window():
    series = [{"date": f"2020-01-0{d}", "value": float(d)} for d in range(1, 4)]
    assert calculate_rolling_average(series, window_size=5) == []


def test_create_daily_series_interpolates_between_points():
    series = [{"date": "2020-01-01", "value": 10.0}, {"date": "2020-01-03", "value": 20.0}]
    out = create_daily_series(series, "2020-01-01", "2020-01-03")
    assert [p["date"] for p in out] == ["2020-01-01", "2020-01-02", "2020-01-03"]
    assert out[1]["value"] == 15.0


def test_drop_duplicate_polls_keys_on_date_pollster_party_value():
    df = pd.DataFrame(
        [
            ("2020-01-01", "X", "A", 10.0),
            ("2020-01-01", "X", "A", 10.0),  # repeat from an overlapping page
            ("2020-01-01", "Y", "A", 10.0),  # different pollster, same value: real poll
            ("2020-01-01", "X", "A", 11.0),  # same pollster and day, other figure
        ],
        columns=["date", "pollster", "party", "polling_value"],
    )
    assert len(drop_duplicate_polls(df)) == 3
