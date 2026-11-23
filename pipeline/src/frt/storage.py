from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from frt.classification import CATEGORIES, is_party_far_right
from frt.config import COUNTRIES_DIR
from frt.postprocessing import (
    calculate_rolling_average,
    create_daily_series,
    drop_duplicate_polls,
    filter_pre_2010_datapoints,
    remove_anomalous_values,
    remove_isolated_datapoints,
)
from frt.series import (
    calculate_latest_total_support_with_parties,
    calculate_support_from_series,
    generate_daily_support_series,
    get_active_parties_for_date,
)

logger = logging.getLogger(__name__)


def save_json(path: Path, data: dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def save_country_polling_csv(
    country: str,
    iso2: str,
    series_by_party: dict,
    party_metadata: dict,
    sources: list,
    updated_at: str,
    latest_total: float | None = None,
) -> None:
    """Save country polling data as CSV files."""
    logger.info(f"Saving polling CSV data for {country} ({iso2})...")
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
                    if "candidate" in df_parties_ref.columns and "party" in df_parties_ref.columns:
                        # Create mapping from candidate name to party name
                        for _, row in df_parties_ref.iterrows():
                            candidate = row.get("candidate", "")
                            party = row.get("party", "")
                            if candidate and party and pd.notna(candidate) and pd.notna(party):
                                candidate_to_party[candidate] = party
                                # Also store party metadata indexed by party name
                                if party not in party_to_metadata:
                                    party_to_metadata[party] = {
                                        "political_position": row.get("political_position", ""),
                                        "ideology": row.get("ideology", ""),
                                        "wikipedia_url": row.get("wikipedia_url", ""),
                                    }
                        logger.info(
                            f"Loaded {len(candidate_to_party)} candidate->party mappings for France"
                        )
                except Exception as e:
                    logger.info(f"Warning: Could not load parties.csv for France: {e}")

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
                    "pollster": point.get("pollster", ""),
                    "party": actual_party_name,
                    "polling_value": point["value"],
                    "political_position": party_info.get("political_position"),
                    "ideology": party_info.get("ideology"),
                    "wikipedia_url": party_info.get("url") or party_info.get("wikipedia_url"),
                }
                # For France, add candidate column
                if country == "France":
                    row_data["candidate"] = candidate_name if candidate_name else ""
                polling_data.append(row_data)

        if polling_data:
            df = pd.DataFrame(polling_data)
            df["date"] = pd.to_datetime(df["date"])

            df = df.sort_values(["party", "date"])

            # Drop repeats from overlapping source pages before neighbours are counted.
            df = drop_duplicate_polls(df)

            # Filter out pre-2010 datapoints
            logger.info(f"Filtering pre-2010 datapoints for {country}...")
            df = filter_pre_2010_datapoints(df, cutoff_year=2010)

            # Remove isolated datapoints
            logger.info(f"Removing isolated datapoints for {country}...")
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
                logger.info(f"Removing anomalous values for {country}...")
                df = remove_anomalous_values(df, threshold=10.0, debug=True)

            if not df.empty:
                df.to_csv(country_dir / "polling_data.csv", index=False)
            else:
                logger.info(
                    f"Warning: All datapoints were removed for {country}, not saving empty CSV"
                )
        else:
            logger.info(f"Warning: No polling data to save for {country}")

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
    iso2: str, categories: list | None = None, latest_total: float | None = None
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
                    df_polling.loc[mask, "political_position"] = info["political_position"]
                    df_polling.loc[mask, "ideology"] = info["ideology"]
                    df_polling.loc[mask, "wikipedia_url"] = info["wikipedia_url"]

        # For France, the data is already saved with party_affiliation in the party column
        # No need to aggregate anymore as it's already done during save

        # Get latest data per party
        latest_data = df_polling.loc[df_polling.groupby("party")["date"].idxmax()]

        # Dynamically classify parties as far-right based on categories
        latest_data["is_far_right"] = latest_data.apply(
            lambda row: is_party_far_right(row["political_position"], row["ideology"], categories),
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
            _, active_parties = result
        else:
            active_parties = []

        logger.info(f"Found {len(active_parties)} active far-right parties")

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
            averaged = calculate_rolling_average(datapoints_by_party[party], window_size=5)

            # Create daily series with interpolation
            if averaged and global_start and global_end:
                series_by_party[party] = create_daily_series(averaged, global_start, global_end)
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
            daily_support_series, active_parties_by_date = generate_daily_support_series(
                series_for_calc,  # All parties' raw data for date determination
                series_by_party,  # Far-right parties' smoothed data for values
                party_metadata_for_calc,
                global_start,
                global_end,
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

        logger.info(
            f"Active parties: {active_parties_for_today} with latest combined support: {recalculated_latest_support:.2f}%"
        )

        # Read metadata
        country_name = iso2  # fallback
        sources = []
        if metadata_json.exists():
            with open(metadata_json, encoding="utf-8") as f:
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
            "latestUpdate": latest_update.strftime("%Y-%m-%d") if latest_update else None,
            "sources": sources,
        }

    except Exception as e:
        logger.info(f"Error reading CSV data for {iso2}: {e}")
        return {}
