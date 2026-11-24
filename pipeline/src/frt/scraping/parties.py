from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from bs4 import BeautifulSoup

from frt.config import COUNTRIES_DIR
from frt.countries import get_country_iso_code
from frt.scraping.http import session
from frt.scraping.sources import HISTORICAL_SEPARATORS
from frt.scraping.user_agent import user_agent

logger = logging.getLogger(__name__)


def clean_text(s: str) -> str:
    # Remove references like [1] or [ 1 ] or  [A] or [ A ]
    s = re.sub(r"\[\s*\w+\s*\]", "", s)
    s = re.sub(r"\[\s*\d+\s*\]", "", s)
    # use HISTORICAL_SEPARATORS
    for sep in HISTORICAL_SEPARATORS:
        if sep in s:
            s = s.split(sep, 1)[0].strip()
    return re.sub(r"\s+", " ", s).strip()


def _extract_party_name(soup: BeautifulSoup) -> str | None:
    h1 = soup.find("h1")
    if h1:
        party_display_name = h1.get_text(" ", strip=True)
        # remove anything in parentheses
        party_display_name = (
            re.sub(r"\s*\(.*?\)\s*", "", party_display_name).strip().replace("–", "-")
        )
        return party_display_name
    return None


def _fetch_political_position(url: str) -> dict | None:
    try:
        headers = {
            "User-Agent": user_agent(),
        }
        r = session.get(url, timeout=30, headers=headers)
        r.raise_for_status()
    except Exception:
        logger.info(f"Error fetching URL: {url}")
        return None
    soup = BeautifulSoup(r.text, "lxml")
    # extract party_name from h1 header
    party_display_name = _extract_party_name(soup)

    # retrieve the following table <table class="infobox vcard">
    table = soup.select_one("table.infobox.vcard") or soup.select_one("table.infobox")
    if not table:
        if logger.isEnabledFor(logging.DEBUG):
            logger.info(f"No infobox found in {url}")
        return None
    # Find row with header 'Political position'
    political_position = None
    ideology = None
    for row in table.select("tr"):
        th = row.find("th")
        if not th:
            continue
        label = th.get_text(" ", strip=True).lower().strip()

        if label.startswith("political"):
            td = row.find("td")
            if not td:
                return None
            text = td.get_text(" ", strip=True)
            political_position = clean_text(text)

        if label.startswith("ideology"):
            td = row.find("td")
            if not td:
                return None
            text = td.get_text(" ", strip=True)
            ideology = clean_text(text)

    if political_position or ideology:
        return {
            "party_display_name": party_display_name,
            "political_position": political_position,
            "ideology": ideology,
            "url": url,
        }
    return None


def _party_url(link: str) -> str | None:
    if not link:
        return None
    if link.startswith("http://") or link.startswith("https://"):
        return link
    # ensure single slash between host and path
    return f"https://en.wikipedia.org/{link.lstrip('/')}"


def annotate_parties_positions(
    country: str, parties: list[dict[str, str]], categories: list[str]
) -> dict:
    """
    For a given country (name), and a list of party dicts containing at least 'name' and optionally 'link',
    fetch the 'Political position' from each party's Wikipedia infobox (infobox vcard).
    Cache results in data/countries/{ISO2}/parties.csv so subsequent runs only fetch missing entries.
    Returns a dict with:
    - 'selected_parties': list of party names whose political position matches categories
    - 'party_metadata': dict of {party_name: {political_position, ideology, url, is_far_right}}
    Also updates parties.csv with all positions.
    """
    # Get ISO2 code for the country
    iso2 = get_country_iso_code(country)
    country_dir = COUNTRIES_DIR / iso2
    country_dir.mkdir(exist_ok=True)

    # Load cache from parties.csv
    cache_path = country_dir / "parties.csv"
    country_cache: dict[str, dict[str, str]] = {}

    if cache_path.exists():
        try:
            df_cache = pd.read_csv(cache_path)
            for _, row in df_cache.iterrows():
                party_name = row.get("party", "")
                if party_name:
                    country_cache[party_name] = {
                        "name": party_name,
                        "party_display_name": row.get("party_display_name"),
                        "political_position": row.get("political_position"),
                        "ideology": row.get("ideology"),
                        "url": row.get("wikipedia_url"),
                    }
        except Exception as e:
            logger.info(f"Error loading cache from {cache_path}: {e}")

    selected_parties: list[str] = []
    party_metadata: dict[str, dict] = {}
    party_name_mapping: dict[str, str] = {}  # maps table name to article name

    # Fetch the uncached party pages concurrently; results are consumed in table order below.
    pending: dict[str, str] = {}
    for p in parties:
        name = str(p.get("name") or "").strip()
        url = _party_url((p.get("link") or "").strip())
        if name and url and name not in country_cache and name not in pending:
            pending[name] = url
    fetched: dict[str, dict | None] = {}
    if pending:
        with ThreadPoolExecutor(max_workers=4) as pool:
            fetched = dict(
                zip(pending, pool.map(_fetch_political_position, pending.values()), strict=True)
            )

    for p in parties:
        party_name_from_table = str(p.get("name") or "").strip()
        link = (p.get("link") or "").strip()
        if not party_name_from_table:
            continue
        party_description = country_cache.get(party_name_from_table)

        # Only fetch from Wikipedia if party is NOT already in the cache
        if not party_description:
            url = _party_url(link)

            if url:
                party_description = fetched[party_name_from_table]

                if party_description:
                    # Add to cache only if it was newly fetched
                    country_cache[party_name_from_table] = party_description
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.info(f"Fetched and cached new party: {party_name_from_table}")

            # If no link or fetch failed, add party to cache with empty data
            if not party_description:
                party_description = {
                    "name": party_name_from_table,
                    "party_display_name": party_name_from_table,
                    "political_position": None,
                    "ideology": None,
                    "url": None,
                }
                country_cache[party_name_from_table] = party_description
                if logger.isEnabledFor(logging.DEBUG):
                    logger.info(f"Added new party with no Wikipedia data: {party_name_from_table}")
        else:
            # Party already exists in cache, use cached data
            if logger.isEnabledFor(logging.DEBUG):
                logger.info(f"Using cached data for: {party_name_from_table}")

        # Determine the canonical party name (prefer article name if available)
        party_name_canonical = party_name_from_table
        if party_description and party_description.get("party_display_name"):
            party_display_name = party_description.get("party_display_name")
            if party_display_name and party_name_from_table not in party_name_mapping:
                party_name_canonical = party_display_name
                if logger.isEnabledFor(logging.DEBUG):
                    logger.info(
                        f"Mapping party name: {party_name_from_table} -> {party_display_name}"
                    )
                party_name_mapping[party_name_from_table] = party_display_name

        # Store party metadata
        is_far_right = False
        if party_description:
            political_position = party_description.get("political_position")
            if political_position and isinstance(political_position, str):
                political_position = political_position.strip().lower().replace(" ", "-")
            else:
                political_position = ""
            ideology = party_description.get("ideology")
            if ideology and isinstance(ideology, str):
                ideology = ideology.strip().lower().replace(" ", "-")
            else:
                ideology = ""

            # Check if selected
            for category in categories:
                if category in political_position or category in ideology:
                    selected_parties.append(party_name_canonical)
                    is_far_right = True
                    break

            party_metadata[party_name_canonical] = {
                "party_display_name": party_description.get("party_display_name"),
                "political_position": party_description.get("political_position"),
                "ideology": party_description.get("ideology"),
                "url": party_description.get("url"),
                "is_far_right": is_far_right,
            }
        else:
            party_metadata[party_name_canonical] = {
                "party_display_name": None,
                "political_position": None,
                "ideology": None,
                "url": None,
                "is_far_right": False,
            }

    # Save updated cache to parties.csv (skip for France as it's manually maintained)
    if country_cache and country != "France":
        try:
            cache_rows = []
            for party_name, info in country_cache.items():
                cache_rows.append(
                    {
                        "party": party_name,
                        "party_display_name": info.get("party_display_name"),
                        "display_name": info.get("display_name"),
                        "political_position": info.get("political_position"),
                        "ideology": info.get("ideology"),
                        "wikipedia_url": info.get("url"),
                    }
                )
            df_cache = pd.DataFrame(cache_rows)
            df_cache.to_csv(cache_path, index=False)
            if logger.isEnabledFor(logging.DEBUG):
                logger.info(f"Saved parties.csv for {country}")
        except Exception as e:
            logger.info(f"Error saving cache to {cache_path}: {e}")
    elif country == "France":
        if logger.isEnabledFor(logging.DEBUG):
            logger.info("Skipping parties.csv save for France (manually maintained)")

    return {
        "selected_parties": selected_parties,
        "party_metadata": party_metadata,
        "party_name_mapping": party_name_mapping,
    }
