from __future__ import annotations
import json
from pathlib import Path
from typing import Optional
import yaml
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
    get_best_polling_source,
    update_summary_with_far_right,
)
import time


ROOT = Path(__file__).resolve().parents[2]
CATEGORIES = ["far-right", "right-wing-populism", "nationalism"]


def build(selected_country: Optional[str] = None):
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

    print("Loading sources config...")
    sources_cfg_path = ROOT / "data-pipeline" / "pipeline" / "sources.yaml"
    sources_cfg = {}
    if sources_cfg_path.exists():
        sources_cfg = yaml.safe_load(sources_cfg_path.read_text()) or {}

    # Load existing summary.json to only update specific countries when we have data
    summary_path = DATA_DIR / "summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            summary = {"updatedAt": now_iso(), "countries": {}}
    else:
        summary = {"updatedAt": now_iso(), "countries": {}}

    for country in countries:
        if selected_country and country != selected_country:
            continue
        if not selected_country:
            # wait 5 seconds between countries to avoid overwhelming the server

            time.sleep(3)
        print(f"\nProcessing {country}...")

        override = None
        if isinstance(sources_cfg.get(country), dict):
            polling_cfg = sources_cfg[country].get("polling")
            if isinstance(polling_cfg, list) and polling_cfg:
                # Only first supported source used for now
                if polling_cfg[0].get("type") == "wikipedia":
                    override = polling_cfg[0].get("url")
        source_type, url = get_best_polling_source(country, override_url=override)
        latest_total = None
        series_by_party = {}
        sources = []
        if source_type == "wikipedia":
            if url:
                fetcher = WikipediaPollingFetcher(url)
                print(f"Fetching latest support data from {url} for {country}...")
                latest_total, series_by_party = fetcher.fetch_latest_and_series(
                    country, CATEGORIES
                )
                sources.append({"type": "wikipedia", "url": url})
            else:
                sources.append({"type": "wikipedia", "url": None})

        party_list = list(series_by_party.keys()) if series_by_party else []

        iso2 = get_country_iso_code(country)

        # Only update summary for this country if we have new data (not None, {})
        has_new = (latest_total is not None) or (bool(series_by_party))
        if has_new:
            # Derive party list from series keys if available, else from scraped list
            entry = {
                "country": country,
                "iso2": iso2,
                "parties": party_list,
                "latestSupport": latest_total,
                "latestUpdate": now_iso(),
            }
            if "countries" not in summary:
                summary["countries"] = {}
            summary["countries"][iso2] = entry

        # Persist country file as before (optional)
        country_data = {
            "country": country,
            "parties": party_list,
            "latestSupport": latest_total,
            "seriesByParty": series_by_party,
            "sources": sources,
            "updatedAt": now_iso(),
        }
        save_json(COUNTRIES_DIR / f"{iso2}.json", country_data)

    # Update top-level timestamp and save summary.json
    summary["updatedAt"] = now_iso()
    save_json(summary_path, summary)

    # Optionally refine summary using cached far-right classification
    try:
        update_summary_with_far_right(summary)
    except Exception as e:
        print(f"Warning: update_summary_with_far_right failed: {e}")

    print("Done.")


if __name__ == "__main__":
    # retrieve country as an argument if needed
    import sys

    if len(sys.argv) > 1:
        country_arg = sys.argv[1]
        build(selected_country=country_arg)
    else:
        build()
