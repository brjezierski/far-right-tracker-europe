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
)
import time


ROOT = Path(__file__).resolve().parents[2]
CATEGORIES = ["far-right", "national-conservatism"]


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

    # Save party metadata
    if party_metadata:
        parties_data = []
        for party_name, info in party_metadata.items():
            row_data = {
                "party": party_name,
                "political_position": info.get("political_position"),
                "ideology": info.get("ideology"),
                "wikipedia_url": info.get("url"),
                "party_display_name": info.get("party_display_name", ""),
            }
            # For France, add party_affiliation column
            if country == "France":
                row_data["party_affiliation"] = info.get("party_affiliation", "")
            parties_data.append(row_data)

        if parties_data:
            df_parties = pd.DataFrame(parties_data)

            # Skip saving parties.csv for France (manually maintained)
            if country != "France":
                df_parties.to_csv(country_dir / "parties.csv", index=False)

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

        # Use provided latest_total if available, otherwise calculate it
        active_parties = []
        if latest_total is not None:
            latest_far_right_support = latest_total
            # If latest_total is provided, assume all selected_parties are active
            active_parties = selected_parties
        else:
            # Build series for ALL parties (to get accurate latest poll dates)
            # But mark only far-right parties as such in metadata
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
                party_metadata_for_calc[party] = {
                    "is_far_right": party in selected_parties
                }

            result = calculate_latest_total_support_with_parties(
                series_for_calc, party_metadata_for_calc
            )
            if result:
                latest_far_right_support, active_parties = result
            else:
                latest_far_right_support = 0.0
                active_parties = []

        print(
            f"Selected parties: {active_parties} with total support {latest_far_right_support}"
        )

        # Generate seriesByParty for far-right parties
        series_by_party = {}
        latest_update = None
        for party in selected_parties:
            party_data = df_polling[df_polling["party"] == party].copy()
            party_data = party_data.sort_values("date")

            # Aggregate by date (average if multiple polls on same date)
            aggregated = party_data.groupby("date")["polling_value"].mean()
            series_by_party[party] = [
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "value": float(f"{value:.2f}"),
                }
                for date, value in aggregated.items()
            ]

            # Track the latest date across all parties
            if not party_data.empty:
                party_latest = party_data["date"].max()
                if latest_update is None or party_latest > latest_update:
                    latest_update = party_latest

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
            "latestSupport": float(latest_far_right_support),
            "seriesByParty": series_by_party,
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
                summary["countries"][iso2] = country_data
                summary["updatedAt"] = now_iso()
                save_json(summary_path, summary)
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
