from __future__ import annotations

import json
import logging
import time

from frt.classification import CATEGORIES
from frt.config import COUNTRIES_DIR, DATA_DIR
from frt.countries import get_country_iso_code
from frt.scraping.polling import WikipediaPollingFetcher
from frt.scraping.sources import WIKI_POLLING_PAGES, get_polling_headers, get_polling_source
from frt.series import calculate_latest_total_support
from frt.storage import now_iso, read_country_data_from_csv, save_country_polling_csv, save_json
from frt.summary import rebuild_summary_from_csv, summarize_country

logger = logging.getLogger(__name__)


def build(selected_country: str | None = None, no_scraping: bool = False):
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
                summary["countries"][iso2] = summarize_country(country_data)
                summary["updatedAt"] = now_iso()
                save_json(summary_path, summary)
                logger.info(f"Updated {selected_country} ({iso2})")
            else:
                logger.info(f"No data found for {selected_country} ({iso2})")
        else:
            # Rebuild entire summary
            rebuild_summary_from_csv()
        return

    if selected_country:
        logger.info(f"Building for specific country: {selected_country}")
    logger.info("Fetching country list...")
    countries = list(WIKI_POLLING_PAGES)
    logger.info(f"Found {len(countries)} countries.")

    for country in countries:
        if selected_country and country != selected_country:
            continue
        # if not selected_country:
        #     # wait 5 seconds between countries to avoid overwhelming the server
        #     time.sleep(3)
        logger.info(f"\nProcessing {country}...")

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
                logger.info(f"Fetching latest support data from {url} for {country}...")
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
            latest_total = calculate_latest_total_support(series_by_party, party_metadata)

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
    logger.info("\nUpdating summary from CSV files...")

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
            summary["countries"][iso2] = summarize_country(country_data)
            logger.info(f"Updated summary for {selected_country} ({iso2})")
        else:
            logger.info(f"No data found for {selected_country} ({iso2})")
    else:
        # Process all country directories to build summary
        summary["countries"] = {}  # Reset countries when updating all
        if COUNTRIES_DIR.exists():
            for country_dir in COUNTRIES_DIR.iterdir():
                if country_dir.is_dir() and len(country_dir.name) == 2:  # ISO2 code
                    iso2 = country_dir.name
                    country_data = read_country_data_from_csv(iso2, CATEGORIES)
                    if country_data:
                        summary["countries"][iso2] = summarize_country(country_data)

    # Update timestamp and save
    summary["updatedAt"] = now_iso()
    save_json(summary_path, summary)

    logger.info("Done.")
