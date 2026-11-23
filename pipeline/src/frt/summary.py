from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from frt.classification import CATEGORIES, is_party_far_right
from frt.config import COUNTRIES_DIR, DATA_DIR
from frt.storage import now_iso, read_country_data_from_csv, save_json

logger = logging.getLogger(__name__)


def summarize_country(country_data: dict) -> dict:
    """Return the fields the map needs; full series stay in the per-country file."""
    return {
        "country": country_data["country"],
        "iso2": country_data["iso2"],
        "parties": country_data["parties"],
        "activeParties": country_data.get("activeParties", []),
        "latestSupport": country_data["latestSupport"],
    }


def rebuild_summary_from_csv(selected_country=None):
    """Rebuild summary.json based on saved country JSON files to ensure consistency."""
    summary_path = DATA_DIR / "summary.json"
    # Keep the last scrape time so a rebuild does not look like fresh data.
    try:
        updated_at = json.loads(summary_path.read_text(encoding="utf-8"))["updatedAt"]
    except (OSError, ValueError, KeyError):
        updated_at = now_iso()
    summary = {"updatedAt": updated_at, "countries": {}, "parties": {}}
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
                summary["countries"][iso2] = summarize_country(country_data)

                # Collect all party metadata
                parties_csv_path = country_dir / "parties.csv"
                if parties_csv_path.exists():
                    parties_df = pd.read_csv(parties_csv_path)
                    for _, party in parties_df.iterrows():
                        party_id = party.get("party_id", party["party"])
                        party_metadata[party_id] = {
                            "party": party["party"],
                            "ideology": party.get("ideology", "Unknown"),
                            "political_position": party.get("political_position", "Unknown"),
                            "is_far_right": is_party_far_right(
                                party.get("political_position", ""),
                                party.get("ideology", ""),
                                CATEGORIES,
                            ),
                        }
        except Exception as e:
            logger.info(f"Error processing country {iso2}: {e}")

    # Save updated summary
    save_json(summary_path, summary)
    logger.info("Updated summary.json")

    if selected_country:
        logger.info(f"Updated country files for: {selected_country}")
