from __future__ import annotations
import json
from pathlib import Path
from typing import Optional
import pandas as pd
from .utils import (
    DATA_DIR,
    PIPELINE_DIR,
    COUNTRIES_DIR,
    get_country_iso_code,
    save_json,
    now_iso,
)
from .polling import (
    WikipediaPollingFetcher,
    get_polling_source,
    get_polling_headers,
    calculate_latest_total_support,
    calculate_latest_total_support_with_parties,
)
from .postprocessing import (
    remove_isolated_datapoints,
    remove_anomalous_values,
    filter_pre_2010_datapoints,
    calculate_rolling_average,
    create_daily_series,
)
import time


ROOT = Path(__file__).resolve().parents[2]
CATEGORIES = ["far-right", "national-conservatism"]


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
    from datetime import datetime

    target_dt = datetime.strptime(target_date, "%Y-%m-%d")

    # Collect all unique polling dates across all parties that are <= target_date
    all_dates = set()
    for party, datapoints in datapoints_by_party.items():
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
    from datetime import datetime, timedelta

    daily_support = []
    active_parties_by_date = {}

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_dt:
        date_str = current_date.strftime("%Y-%m-%d")

        # Get active parties for this date
        active_parties = get_active_parties_for_date(
            datapoints_by_party, party_metadata, date_str
        )
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
    from datetime import datetime, date as date_module

    # Determine active parties based on target date
    if target_date is None:
        target_date = date_module.today().strftime("%Y-%m-%d")

    active_parties = get_active_parties_for_date(
        datapoints_by_party, party_metadata, target_date
    )

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


def is_party_far_right(
    political_position: str, ideology: str, categories: list
) -> bool:
    """Determine if a party is far-right based on political position and ideology."""
    if not political_position and not ideology:
        return False

    if pd.isna(political_position):
        political_position = ""
    if pd.isna(ideology):
        ideology = ""

    # Normalize text for comparison
    position_normalized = (political_position or "").strip().lower().replace(" ", "-")
    ideology_normalized = (ideology or "").strip().lower().replace(" ", "-")

    # Check if any category matches
    for category in categories:
        if category in position_normalized or category in ideology_normalized:
            return True

    return False


def save_country_polling_csv(
    country: str,
    iso2: str,
    series_by_party: dict,
    party_metadata: dict,
    sources: list,
    updated_at: str,
    latest_total: Optional[float] = None,
) -> None:
    """Save country polling data as CSV files."""
    print(f"Saving polling CSV data for {country} ({iso2})...")
    # Create country-specific directory
    country_dir = COUNTRIES_DIR / iso2
    country_dir.mkdir(exist_ok=True)

    # Save polling time series data
    if series_by_party:
        # For France, load the manually maintained parties.csv to get candidate->party mapping
        candidate_to_party = {}
        party_to_metadata = {}  # Store metadata by party name for France
        if country == "France":
            parties_csv_path = country_dir / "parties.csv"
            if parties_csv_path.exists():
                try:
                    df_parties_ref = pd.read_csv(parties_csv_path)
                    if (
                        "candidate" in df_parties_ref.columns
                        and "party" in df_parties_ref.columns
                    ):
                        # Create mapping from candidate name to party name
                        for _, row in df_parties_ref.iterrows():
                            candidate = row.get("candidate", "")
                            party = row.get("party", "")
                            if (
                                candidate
                                and party
                                and pd.notna(candidate)
                                and pd.notna(party)
                            ):
                                candidate_to_party[candidate] = party
                                # Also store party metadata indexed by party name
                                if party not in party_to_metadata:
                                    party_to_metadata[party] = {
                                        "political_position": row.get(
                                            "political_position", ""
                                        ),
                                        "ideology": row.get("ideology", ""),
                                        "wikipedia_url": row.get("wikipedia_url", ""),
                                    }
                        print(
                            f"Loaded {len(candidate_to_party)} candidate->party mappings for France"
                        )
                except Exception as e:
                    print(f"Warning: Could not load parties.csv for France: {e}")

        polling_data = []
        for party_name, time_series in series_by_party.items():
            party_info = party_metadata.get(party_name, {})

            # For France, map candidate name to party name
            actual_party_name = party_name
            candidate_name = None
            if country == "France" and party_name in candidate_to_party:
                candidate_name = party_name
                actual_party_name = candidate_to_party[party_name]
                # Get party info from the parties.csv metadata
                if actual_party_name in party_to_metadata:
                    party_info = party_to_metadata[actual_party_name]

            for point in time_series:
                row_data = {
                    "date": point["date"],
                    "party": actual_party_name,
                    "polling_value": point["value"],
                    "political_position": party_info.get("political_position"),
                    "ideology": party_info.get("ideology"),
                    "wikipedia_url": party_info.get("url")
                    or party_info.get("wikipedia_url"),
                }
                # For France, add candidate column
                if country == "France":
                    row_data["candidate"] = candidate_name if candidate_name else ""
                polling_data.append(row_data)

        if polling_data:
            df = pd.DataFrame(polling_data)
            df["date"] = pd.to_datetime(df["date"])

            df = df.sort_values(["party", "date"])

            # Filter out pre-2010 datapoints
            print(f"Filtering pre-2010 datapoints for {country}...")
            df = filter_pre_2010_datapoints(df, cutoff_year=2010)

            # Remove isolated datapoints
            print(f"Removing isolated datapoints for {country}...")
            df = remove_isolated_datapoints(df, min_neighbors=2)

            # Remove anomalous values for specific countries
            countries_for_anomaly_removal = [
                "Spain",
                "Austria",
                "Poland",
                "Czech Republic",
                "Portugal",
            ]
            if country in countries_for_anomaly_removal:
                print(f"Removing anomalous values for {country}...")
                df = remove_anomalous_values(df, threshold=10.0, debug=True)

            if not df.empty:
                df.to_csv(country_dir / "polling_data.csv", index=False)
            else:
                print(
                    f"Warning: All datapoints were removed for {country}, not saving empty CSV"
                )
        else:
            print(f"Warning: No polling data to save for {country}")

    # Note: party metadata is now saved by annotate_parties_positions() to preserve manual edits
    # We no longer save parties.csv here to avoid overwriting manual changes

    # Save metadata as JSON for backward compatibility
    metadata = {
        "country": country,
        "iso2": iso2,
        "sources": sources,
        "updatedAt": updated_at,
    }

    # Add latest_total if provided (for new calculation method)
    if latest_total is not None:
        metadata["latestTotal"] = latest_total

    with open(country_dir / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)


def read_country_data_from_csv(
    iso2: str, categories: Optional[list] = None, latest_total: Optional[float] = None
) -> dict:
    """Read country data from CSV files and return summary information."""
    if categories is None:
        categories = CATEGORIES

    country_dir = COUNTRIES_DIR / iso2

    if not country_dir.exists():
        return {}

    polling_csv = country_dir / "polling_data.csv"
    metadata_json = country_dir / "metadata.json"

    if not polling_csv.exists():
        return {}

    try:
        # Read polling data
        df_polling = pd.read_csv(polling_csv)
        df_polling["date"] = pd.to_datetime(df_polling["date"])

        # Update ideology and political_position from parties.csv (to respect manual edits)
        parties_csv = country_dir / "parties.csv"
        if parties_csv.exists():
            df_parties = pd.read_csv(parties_csv)
            party_info_map = {}
            for _, row in df_parties.iterrows():
                party_name = row.get("party", "")
                if party_name:
                    party_info_map[party_name] = {
                        "political_position": row.get("political_position", ""),
                        "ideology": row.get("ideology", ""),
                        "wikipedia_url": row.get("wikipedia_url", ""),
                    }

            # Update polling data with latest party info from parties.csv
            for party_name, info in party_info_map.items():
                mask = df_polling["party"] == party_name
                if mask.any():
                    df_polling.loc[mask, "political_position"] = info[
                        "political_position"
                    ]
                    df_polling.loc[mask, "ideology"] = info["ideology"]
                    df_polling.loc[mask, "wikipedia_url"] = info["wikipedia_url"]

        # For France, the data is already saved with party_affiliation in the party column
        # No need to aggregate anymore as it's already done during save

        # Get latest data per party
        latest_data = df_polling.loc[df_polling.groupby("party")["date"].idxmax()]

        # Dynamically classify parties as far-right based on categories
        latest_data["is_far_right"] = latest_data.apply(
            lambda row: is_party_far_right(
                row["political_position"], row["ideology"], categories
            ),
            axis=1,
        )

        # Calculate far-right totals
        selected_parties = latest_data[latest_data["is_far_right"]]["party"].tolist()

        # Build series for ALL parties to determine which have recent polling data
        series_for_calc = {}
        party_metadata_for_calc = {}

        # Include ALL parties to determine latest poll dates accurately
        all_parties = df_polling["party"].unique()
        for party in all_parties:
            party_data = df_polling[df_polling["party"] == party].copy()
            party_data = party_data.sort_values("date")
            series_for_calc[party] = [
                {
                    "date": row["date"].strftime("%Y-%m-%d"),
                    "value": row["polling_value"],
                }
                for _, row in party_data.iterrows()
            ]
            # Mark as far-right only if in selected_parties
            party_metadata_for_calc[party] = {"is_far_right": party in selected_parties}

        # Always use calculate_latest_total_support_with_parties to determine active parties
        result = calculate_latest_total_support_with_parties(
            series_for_calc, party_metadata_for_calc
        )
        if result:
            latest_far_right_support, active_parties = result
        else:
            latest_far_right_support = 0.0
            active_parties = []

        print(f"Found {len(active_parties)} active far-right parties")

        # Generate both datapointsByParty (raw polls) and seriesByParty (rolling average + daily interpolation)
        datapoints_by_party = {}
        series_by_party = {}
        latest_update = None

        # Find earliest and latest dates across all far-right parties for consistent date range
        all_dates = []
        for party in selected_parties:
            party_data = df_polling[df_polling["party"] == party]
            if not party_data.empty:
                all_dates.extend(party_data["date"].tolist())

        if all_dates:
            global_start = min(all_dates).strftime("%Y-%m-%d")
            global_end = max(all_dates).strftime("%Y-%m-%d")
        else:
            global_start = None
            global_end = None

        for party in selected_parties:
            party_data = df_polling[df_polling["party"] == party].copy()
            party_data = party_data.sort_values("date")

            # Aggregate by date (average if multiple polls on same date)
            aggregated = party_data.groupby("date")["polling_value"].mean()

            # Raw datapoints for datapointsByParty
            datapoints_by_party[party] = [
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "value": float(f"{value:.2f}"),
                }
                for date, value in aggregated.items()
            ]

            # Calculate rolling average (5-point window)
            averaged = calculate_rolling_average(
                datapoints_by_party[party], window_size=5
            )

            # Create daily series with interpolation
            if averaged and global_start and global_end:
                series_by_party[party] = create_daily_series(
                    averaged, global_start, global_end
                )
            else:
                series_by_party[party] = []

            # Track the latest date across all parties
            if not party_data.empty:
                party_latest = party_data["date"].max()
                if latest_update is None or party_latest > latest_update:
                    latest_update = party_latest

        # Generate daily support series and active parties by date
        daily_support_series = []
        active_parties_by_date = {}
        if global_start and global_end and selected_parties:
            # Use series_for_calc which contains ALL parties for determining active dates
            # But only for far-right parties in the actual calculation
            daily_support_series, active_parties_by_date = (
                generate_daily_support_series(
                    series_for_calc,  # All parties' raw data for date determination
                    series_by_party,  # Far-right parties' smoothed data for values
                    party_metadata_for_calc,
                    global_start,
                    global_end,
                )
            )

        # Calculate latestSupport using the same logic as the map tooltip
        # This sums the latest values from seriesByParty for all active parties at today's date
        from datetime import date

        today = date.today().strftime("%Y-%m-%d")
        recalculated_latest_support = calculate_support_from_series(
            datapoints_by_party,
            series_by_party,
            party_metadata_for_calc,
            target_date=today,
        )

        # Get active parties for logging
        active_parties_for_today = get_active_parties_for_date(
            datapoints_by_party, party_metadata_for_calc, today
        )

        print(
            f"Active parties: {active_parties_for_today} with latest combined support: {recalculated_latest_support:.2f}%"
        )

        # Read metadata
        country_name = iso2  # fallback
        sources = []
        if metadata_json.exists():
            with open(metadata_json, "r", encoding="utf-8") as f:
                metadata = json.load(f)
                country_name = metadata.get("country", iso2)
                sources = metadata.get("sources", [])

        return {
            "country": country_name,
            "iso2": iso2,
            "parties": selected_parties,
            "activeParties": active_parties,
            "latestSupport": float(recalculated_latest_support),
            "datapointsByParty": datapoints_by_party,
            "seriesByParty": series_by_party,
            "dailySupportSeries": daily_support_series,
            "activePartiesByDate": active_parties_by_date,
            "latestUpdate": latest_update.strftime("%Y-%m-%d")
            if latest_update
            else None,
            "sources": sources,
        }

    except Exception as e:
        print(f"Error reading CSV data for {iso2}: {e}")
        return {}


def rebuild_summary_from_csv(selected_country=None):
    """Rebuild summary.json based on saved country JSON files to ensure consistency."""
    summary = {"countries": {}, "parties": {}}
    party_metadata = {}  # Keep track of all party metadata

    country_dirs = list(Path(COUNTRIES_DIR).glob("*/"))

    for country_dir in country_dirs:
        iso2 = country_dir.name

        if selected_country:
            # Check if this country should be updated
            if selected_country.lower() not in [iso2.lower()]:
                continue

        # First, regenerate country JSON file from CSV to ensure it has latest fields
        country_json_path = COUNTRIES_DIR / f"{iso2}.json"
        try:
            country_data = read_country_data_from_csv(iso2, CATEGORIES)
            if country_data:
                save_json(country_json_path, country_data)
                # For summary, only include necessary fields (not seriesByParty)
                summary["countries"][iso2] = {
                    "country": country_data["country"],
                    "iso2": country_data["iso2"],
                    "parties": country_data["parties"],
                    "activeParties": country_data.get("activeParties", []),
                    "latestSupport": country_data["latestSupport"],
                }

                # Collect all party metadata
                parties_csv_path = country_dir / "parties.csv"
                if parties_csv_path.exists():
                    parties_df = pd.read_csv(parties_csv_path)
                    for _, party in parties_df.iterrows():
                        party_id = party.get("party_id", party["party"])
                        party_metadata[party_id] = {
                            "party": party["party"],
                            "ideology": party.get("ideology", "Unknown"),
                            "political_position": party.get(
                                "political_position", "Unknown"
                            ),
                            "is_far_right": is_party_far_right(
                                party.get("political_position", ""),
                                party.get("ideology", ""),
                                CATEGORIES,
                            ),
                        }
        except Exception as e:
            print(f"Error processing country {iso2}: {e}")

    # Save updated summary
    save_json(DATA_DIR / "summary.json", summary)
    print("Updated summary.json")

    if selected_country:
        print(f"Updated country files for: {selected_country}")


def build(selected_country: Optional[str] = None, no_scraping: bool = False):
    if no_scraping:
        if selected_country:
            # Load existing summary or create new one
            summary_path = DATA_DIR / "summary.json"
            if summary_path.exists():
                try:
                    summary = json.loads(summary_path.read_text(encoding="utf-8"))
                except Exception:
                    summary = {"updatedAt": now_iso(), "countries": {}}
            else:
                summary = {"updatedAt": now_iso(), "countries": {}}

            # Only update the specific country
            iso2 = get_country_iso_code(selected_country)
            country_data = read_country_data_from_csv(iso2, CATEGORIES)
            if country_data:
                # Save individual country JSON file
                country_json_path = COUNTRIES_DIR / f"{iso2}.json"
                save_json(country_json_path, country_data)

                # Update summary
                summary["countries"][iso2] = country_data
                summary["updatedAt"] = now_iso()
                save_json(summary_path, summary)
                print(f"Updated {selected_country} ({iso2})")
            else:
                print(f"No data found for {selected_country} ({iso2})")
        else:
            # Rebuild entire summary
            rebuild_summary_from_csv()
        return

    if selected_country:
        print(f"Building for specific country: {selected_country}")
    print("Fetching country list...")
    # read polling_sources.json
    polling_sources_path = PIPELINE_DIR / "polling_sources.json"
    if polling_sources_path.exists():
        with open(polling_sources_path, "r", encoding="utf-8") as f:
            polling_sources = json.load(f)
    else:
        polling_sources = {}
    # get list of countries from the keys of polling_sources.json
    countries = list(polling_sources.keys())
    print(f"Found {len(countries)} countries.")

    for country in countries:
        if selected_country and country != selected_country:
            continue
        # if not selected_country:
        #     # wait 5 seconds between countries to avoid overwhelming the server
        #     time.sleep(3)
        print(f"\nProcessing {country}...")

        urls = get_polling_source(country)
        series_by_party = {}
        party_metadata = {}
        sources = []
        latest_total = None

        if urls:
            # Get headers for this country (applies to all URLs)
            headers = get_polling_headers(country)

            # Process each URL and merge the data
            for url in urls:
                # wait 3 seconds between countries to avoid overwhelming the server
                time.sleep(3)

                fetcher = WikipediaPollingFetcher(url, headers)
                print(f"Fetching latest support data from {url} for {country}...")
                url_latest_total, url_series_by_party, url_party_metadata = (
                    fetcher.fetch_latest_and_series(country, CATEGORIES)
                )

                # Merge series data
                for party, points in url_series_by_party.items():
                    if party not in series_by_party:
                        series_by_party[party] = []
                    series_by_party[party].extend(points)

                # Merge party metadata (later URLs override earlier ones)
                party_metadata.update(url_party_metadata)

                # Keep track of all sources
                sources.append(url)

        # Recalculate latest total support based on merged data
        if series_by_party and party_metadata:
            latest_total = calculate_latest_total_support(
                series_by_party, party_metadata
            )

        iso2 = get_country_iso_code(country)

        # Save country data as CSV files first
        save_country_polling_csv(
            country=country,
            iso2=iso2,
            series_by_party=series_by_party,
            party_metadata=party_metadata,
            sources=sources,
            updated_at=now_iso(),
            latest_total=latest_total,
        )

        # Also save individual country JSON for frontend compatibility
        country_data = read_country_data_from_csv(iso2, CATEGORIES, latest_total)
        if country_data:
            # Add sources to the country data
            country_data["sources"] = sources
            save_json(COUNTRIES_DIR / f"{iso2}.json", country_data)

    # Now update summary.json using data from CSV files
    print("\nUpdating summary from CSV files...")

    # Load existing summary or create new one
    summary_path = DATA_DIR / "summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            summary = {"updatedAt": now_iso(), "countries": {}}
    else:
        summary = {"updatedAt": now_iso(), "countries": {}}

    if selected_country:
        # Only update the specific country that was processed
        iso2 = get_country_iso_code(selected_country)
        country_data = read_country_data_from_csv(iso2, CATEGORIES)
        if country_data:
            summary["countries"][iso2] = country_data
            print(f"Updated summary for {selected_country} ({iso2})")
        else:
            print(f"No data found for {selected_country} ({iso2})")
    else:
        # Process all country directories to build summary
        summary["countries"] = {}  # Reset countries when updating all
        if COUNTRIES_DIR.exists():
            for country_dir in COUNTRIES_DIR.iterdir():
                if country_dir.is_dir() and len(country_dir.name) == 2:  # ISO2 code
                    iso2 = country_dir.name
                    country_data = read_country_data_from_csv(iso2, CATEGORIES)
                    if country_data:
                        summary["countries"][iso2] = country_data

    # Update timestamp and save
    summary["updatedAt"] = now_iso()
    save_json(summary_path, summary)

    print("Done.")


if __name__ == "__main__":
    # retrieve country as an argument if needed
    import sys
    from .polling import set_debug_mode as set_polling_debug
    from .utils import set_debug_mode as set_utils_debug

    no_scraping = False
    country_arg = None
    debug_mode = False

    # Parse arguments
    args = sys.argv[1:]
    if "--no-scraping" in args:
        no_scraping = True
        args.remove("--no-scraping")

    if "--debug" in args:
        debug_mode = True
        args.remove("--debug")
        set_polling_debug(True)
        set_utils_debug(True)
        print("Debug mode enabled")

    if args:
        country_arg = args[0]

    if country_arg:
        build(selected_country=country_arg, no_scraping=no_scraping)
    else:
        build(no_scraping=no_scraping)
