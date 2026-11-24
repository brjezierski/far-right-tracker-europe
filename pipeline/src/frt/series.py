from __future__ import annotations

import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def load_party_overlaps(iso2: str) -> dict[str, list[str]]:
    """Return {alliance: [member parties]} for a country (see config/party_overlaps.json)."""
    from frt.config import OVERLAPS_PATH

    if not OVERLAPS_PATH.exists():
        return {}
    with open(OVERLAPS_PATH, encoding="utf-8") as f:
        return json.load(f).get(iso2, {})


def hide_overlapping_members(
    datapoints_by_party: dict, series_by_party: dict, overlaps: dict[str, list[str]]
) -> None:
    """Remove member parties' polls and series while their alliance is polled, in place.

    An alliance and its lead party are polled as separate rows (e.g. United Right and Law and
    Justice). Showing both counts the same voters twice, so the members are cut for the span
    between the alliance's first and last poll.
    """
    for alliance, members in overlaps.items():
        polls = datapoints_by_party.get(alliance)
        if not polls:
            continue
        start = min(p["date"] for p in polls)
        end = max(p["date"] for p in polls)
        for member in members:
            for data in (datapoints_by_party, series_by_party):
                if member in data:
                    data[member] = [p for p in data[member] if not start <= p["date"] <= end]


def calculate_latest_total_support(
    series: dict[str, list[dict]], party_metadata: dict[str, dict]
) -> float | None:
    """
    Calculate latest total support as the average of total support from the latest 3 polls.
    Only includes parties marked as far-right (is_far_right=True).

    Args:
        series: {party: [{date, value}]}
        party_metadata: {party: {is_far_right, ...}}

    Returns:
        Average of sum of far-right party support across latest 3 poll dates, or None if no data
    """
    result = calculate_latest_total_support_with_parties(series, party_metadata)
    return result[0] if result else None


def calculate_latest_total_support_with_parties(
    series: dict[str, list[dict]], party_metadata: dict[str, dict]
) -> tuple[float, list[str]] | None:
    """
    Calculate latest total support and return which parties contributed.
    Only includes parties marked as far-right (is_far_right=True).

    Args:
        series: {party: [{date, value}]}
        party_metadata: {party: {is_far_right, ...}}

    Returns:
        Tuple of (average support, list of active parties) or None if no data
    """
    # Get all unique dates
    all_dates_set = set()
    for pts in series.values():
        for pt in pts:
            all_dates_set.add(pt["date"])

    if not all_dates_set:
        return None

    # Sort dates in descending order (most recent first)
    sorted_dates = sorted(list(all_dates_set), reverse=True)
    latest_3_dates = sorted_dates[:3]

    # Track which parties contributed to the latest polls
    active_parties = set()

    # Calculate total support for each of the latest 3 dates (only for far-right parties)
    totals_per_date = []
    for date_str in latest_3_dates:
        date_total = 0
        for party_name, pts in series.items():
            # Only include parties that are marked as far-right
            if party_name in party_metadata and party_metadata[party_name].get(
                "is_far_right", False
            ):
                # Find the polling value for this party on this date
                for pt in pts:
                    if pt["date"] == date_str:
                        date_total += pt["value"]
                        active_parties.add(party_name)
                        break
        totals_per_date.append(date_total)

    # Average the totals
    avg_support = sum(totals_per_date) / len(totals_per_date) if totals_per_date else 0.0
    return (avg_support, sorted(list(active_parties))) if totals_per_date else None


def get_active_parties_for_date(
    datapoints_by_party: dict, party_metadata: dict, target_date: str
) -> list:
    """
    Determine which parties are active based on the target date.
    A party is active if it appears in at least one of the last 3 polling dates before or on the target date.

    Args:
        datapoints_by_party: Dictionary of {party_name: [{date, value}, ...]} (raw polling data)
        party_metadata: Dictionary of {party_name: {is_far_right: bool, ...}}
        target_date: Target date string (YYYY-MM-DD)

    Returns:
        List of party names that are active and far-right
    """

    target_dt = datetime.strptime(target_date, "%Y-%m-%d")

    # Collect all unique polling dates across all parties that are <= target_date
    all_dates = set()
    for datapoints in datapoints_by_party.values():
        for point in datapoints:
            point_dt = datetime.strptime(point["date"], "%Y-%m-%d")
            if point_dt <= target_dt:
                all_dates.add(point["date"])

    if not all_dates:
        return []

    # Sort dates and get the 3 most recent
    sorted_dates = sorted(all_dates, reverse=True)[:3]

    # Find parties that have data in at least one of these 3 dates
    active_parties = []
    for party, datapoints in datapoints_by_party.items():
        # Only consider far-right parties
        if not party_metadata.get(party, {}).get("is_far_right", False):
            continue

        # Check if party has data in any of the 3 most recent dates
        party_dates = {point["date"] for point in datapoints}
        if any(date in party_dates for date in sorted_dates):
            active_parties.append(party)

    return active_parties


def generate_daily_support_series(
    datapoints_by_party: dict,
    series_by_party: dict,
    party_metadata: dict,
    start_date: str,
    end_date: str,
) -> tuple[list, dict]:
    """
    Generate daily support values and active parties for each date.

    Args:
        datapoints_by_party: Dictionary of {party_name: [{date, value}, ...]} (raw polling data)
        series_by_party: Dictionary of {party_name: [{date, value}, ...]} (smoothed daily series)
        party_metadata: Dictionary of {party_name: {is_far_right: bool, ...}}
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)

    Returns:
        Tuple of (daily_support_series, active_parties_by_date)
        - daily_support_series: [{date, value}, ...]
        - active_parties_by_date: {date: [party_names]}
    """
    from datetime import timedelta

    daily_support = []
    active_parties_by_date = {}

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_dt:
        date_str = current_date.strftime("%Y-%m-%d")

        # Get active parties for this date
        active_parties = get_active_parties_for_date(datapoints_by_party, party_metadata, date_str)
        active_parties_by_date[date_str] = active_parties

        # Calculate total support from active parties
        total_support = 0.0
        for party in active_parties:
            if party not in series_by_party:
                continue

            # Find the value for this date in the series
            party_series = series_by_party[party]
            for point in party_series:
                if point["date"] == date_str:
                    total_support += point["value"]
                    break

        daily_support.append({"date": date_str, "value": round(total_support, 2)})

        current_date += timedelta(days=1)

    return daily_support, active_parties_by_date


def calculate_support_from_series(
    datapoints_by_party: dict,
    series_by_party: dict,
    party_metadata: dict,
    target_date: str,
) -> float:
    """
    Calculate total support by summing the latest values from seriesByParty for each active party.
    Active parties are determined internally based on the target date (parties that appear in at least
    one of the last 3 polling dates before or on the target date).

    Args:
        datapoints_by_party: Dictionary of {party_name: [{date, value}, ...]} (raw polling data)
        series_by_party: Dictionary of {party_name: [{date, value}, ...]} (smoothed series)
        party_metadata: Dictionary of {party_name: {is_far_right: bool, ...}}
        target_date: Target date string (YYYY-MM-DD). If None, uses the absolute latest date.

    Returns:
        Total support as a float
    """
    from datetime import date as date_module

    # Determine active parties based on target date
    if target_date is None:
        target_date = date_module.today().strftime("%Y-%m-%d")

    active_parties = get_active_parties_for_date(datapoints_by_party, party_metadata, target_date)

    total_support = 0.0

    for party in active_parties:
        if party not in series_by_party or not series_by_party[party]:
            continue

        series = series_by_party[party]

        # Find the latest data point on or before the target date
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        latest_value = None
        latest_date = None

        for point in series:
            point_dt = datetime.strptime(point["date"], "%Y-%m-%d")
            if point_dt <= target_dt:
                if latest_date is None or point_dt > latest_date:
                    latest_date = point_dt
                    latest_value = point["value"]

        if latest_value is not None:
            total_support += latest_value

    return total_support
