from frt.series import calculate_support_from_series, get_active_parties_for_date


def test_inactive_far_right_party_contributes_no_support():
    # Far-right party last polled long before the 3 latest dates of other parties.
    raw = {
        "Far": [{"date": "2020-01-01", "value": 1.0}],
        "Other": [{"date": f"2024-01-0{d}", "value": 30.0} for d in (1, 2, 3)],
    }
    smoothed = {"Far": [{"date": "2024-01-03", "value": 0.9}]}
    meta = {"Far": {"is_far_right": True}, "Other": {"is_far_right": False}}

    assert get_active_parties_for_date(raw, meta, "2024-01-03") == []
    assert calculate_support_from_series(raw, smoothed, meta, "2024-01-03") == 0.0
