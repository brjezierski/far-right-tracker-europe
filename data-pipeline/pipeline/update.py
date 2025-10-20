from __future__ import annotations
import json
from pathlib import Path
from typing import Optional
import yaml
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
)
import time


ROOT = Path(__file__).resolve().parents[2]
# CATEGORIES = ["communism"]  # , "liberal", "conservatism"]
CATEGORIES = ["far-right", "right-wing-populism", "liberal", "conservatism"]


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
) -> None:
    """Save country polling data as CSV files."""
    # Create country-specific directory
    country_dir = COUNTRIES_DIR / iso2
    country_dir.mkdir(exist_ok=True)

    # Save polling time series data
    if series_by_party:
        polling_data = []
        for party_name, time_series in series_by_party.items():
            party_info = party_metadata.get(party_name, {})
            for point in time_series:
                polling_data.append(
                    {
                        "date": point["date"],
                        "party": party_name,
                        "polling_value": point["value"],
                        "political_position": party_info.get("political_position"),
                        "ideology": party_info.get("ideology"),
                        "wikipedia_url": party_info.get("url"),
                    }
                )

        if polling_data:
            df = pd.DataFrame(polling_data)
            df["date"] = pd.to_datetime(df["date"])
            df = df.dropna(subset=["wikipedia_url"])
            df = df.sort_values(["party", "date"])
            df.to_csv(country_dir / "polling_data.csv", index=False)

    # Save party metadata
    if party_metadata:
        parties_data = []
        for party_name, info in party_metadata.items():
            parties_data.append(
                {
                    "party": party_name,
                    "political_position": info.get("political_position"),
                    "ideology": info.get("ideology"),
                    "wikipedia_url": info.get("url"),
                }
            )

        if parties_data:
            df_parties = pd.DataFrame(parties_data)
            df_parties.to_csv(country_dir / "parties.csv", index=False)

    # Save metadata as JSON for backward compatibility
    metadata = {
        "country": country,
        "iso2": iso2,
        "sources": sources,
        "updatedAt": updated_at,
    }

    with open(country_dir / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)


def read_country_data_from_csv(iso2: str, categories: Optional[list] = None) -> dict:
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
        latest_far_right_support = latest_data[latest_data["is_far_right"]][
            "polling_value"
        ].sum()
        print(
            f"Selected parties: {selected_parties} with total support {latest_far_right_support}"
        )

        # Generate seriesByParty for far-right parties
        series_by_party = {}
        for party in selected_parties:
            party_data = df_polling[df_polling["party"] == party].copy()
            party_data = party_data.sort_values("date")
            series_by_party[party] = [
                {
                    "date": row["date"].strftime("%Y-%m-%d"),
                    "value": row["polling_value"],
                }
                for _, row in party_data.iterrows()
            ]
            # for each party if there is a more than one row with the same date take the average
            # (to handle cases where multiple polls are conducted on the same date)
            series_by_party[party] = [
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "value": float(f"{value:.2f}"),
                }
                for date, value in party_data.groupby("date")["polling_value"]
                .mean()
                .items()
            ]

        # Read metadata
        country_name = iso2  # fallback
        if metadata_json.exists():
            with open(metadata_json, "r", encoding="utf-8") as f:
                metadata = json.load(f)
                country_name = metadata.get("country", iso2)

        return {
            "country": country_name,
            "iso2": iso2,
            "parties": selected_parties,
            "latestSupport": float(latest_far_right_support),
            "seriesByParty": series_by_party,
            "latestUpdate": now_iso(),
        }

    except Exception as e:
        print(f"Error reading CSV data for {iso2}: {e}")
        return {}


def rebuild_summary_from_csv(selected_country=None):
    """Rebuild summary.json based on saved CSV data and categories."""
    summary = {"countries": {}, "parties": {}}
    party_metadata = {}  # Keep track of all party metadata

    country_dirs = list(Path(COUNTRIES_DIR).glob("*/"))

    for country_dir in country_dirs:
        iso2 = country_dir.name

        if selected_country:
            # Check if this country should be updated
            if selected_country.lower() not in [iso2.lower()]:
                continue

        country_data = read_country_data_from_csv(iso2, CATEGORIES)
        if country_data:
            # Create summary data without seriesByParty
            summary_country_data = {
                k: v
                for k, v in country_data.items()  # if k != "seriesByParty"
            }
            summary["countries"][iso2] = summary_country_data

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

            # Create individual country JSON file for frontend compatibility (with seriesByParty)
            country_json_data = dict(country_data)
            save_json(COUNTRIES_DIR / f"{iso2}.json", country_json_data)

    # summary["parties"] = party_metadata

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
        if not selected_country:
            # wait 5 seconds between countries to avoid overwhelming the server
            time.sleep(3)
        print(f"\nProcessing {country}...")

        source_type, url = get_polling_source(country)
        series_by_party = {}
        party_metadata = {}
        sources = []
        if url:
            fetcher = WikipediaPollingFetcher(url)
            print(f"Fetching latest support data from {url} for {country}...")
            latest_total, series_by_party, party_metadata = (
                fetcher.fetch_latest_and_series(country, CATEGORIES)
            )
            sources.append({"type": "wikipedia", "url": url})
        else:
            sources.append({"type": "wikipedia", "url": None})

        iso2 = get_country_iso_code(country)

        # Save country data as CSV files first
        save_country_polling_csv(
            country=country,
            iso2=iso2,
            series_by_party=series_by_party,
            party_metadata=party_metadata,
            sources=sources,
            updated_at=now_iso(),
        )

        # Also save individual country JSON for frontend compatibility
        country_data = read_country_data_from_csv(iso2, CATEGORIES)
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

    no_scraping = False
    country_arg = None

    # Parse arguments
    args = sys.argv[1:]
    if "--no-scraping" in args:
        no_scraping = True
        args.remove("--no-scraping")

    if args:
        country_arg = args[0]

    if country_arg:
        build(selected_country=country_arg, no_scraping=no_scraping)
    else:
        build(no_scraping=no_scraping)
