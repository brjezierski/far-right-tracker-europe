from frt.series import hide_overlapping_members


def _pts(*dates):
    return [{"date": d, "value": 30.0} for d in dates]


def test_member_cut_only_while_alliance_is_polled():
    datapoints = {
        "United Right": _pts("2019-07-24", "2023-10-15"),
        "Law and Justice": _pts("2018-12-01", "2021-01-01", "2023-10-20"),
    }
    series = {
        "United Right": _pts("2019-08-01", "2023-10-15"),
        "Law and Justice": _pts("2018-12-01", "2020-05-05", "2023-10-20"),
    }
    hide_overlapping_members(datapoints, series, {"United Right": ["Law and Justice"]})

    assert [p["date"] for p in datapoints["Law and Justice"]] == ["2018-12-01", "2023-10-20"]
    assert [p["date"] for p in series["Law and Justice"]] == ["2018-12-01", "2023-10-20"]
    assert len(datapoints["United Right"]) == 2 and len(series["United Right"]) == 2


def test_no_change_without_overlap_config_or_alliance_polls():
    datapoints = {"Law and Justice": _pts("2020-01-01")}
    series = {"Law and Justice": _pts("2020-01-01")}
    hide_overlapping_members(datapoints, series, {})
    hide_overlapping_members(datapoints, series, {"United Right": ["Law and Justice"]})
    assert len(series["Law and Justice"]) == 1
