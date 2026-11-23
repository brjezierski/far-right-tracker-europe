"""Which Wikipedia pages and table headers to scrape for each country."""

from __future__ import annotations

import json

from frt.config import SOURCES_PATH

WIKI_POLLING_PAGES: dict[str, dict] = json.loads(SOURCES_PATH.read_text(encoding="utf-8"))

COUNTRY_EXCLUDED_HEADERS = {
    "Denmark": ["Constituency polling"],
    "Poland": ["Alternative scenarios", "Parties"],
}
HISTORICAL_SEPARATORS = ["Formerly:", "Historical"]


def get_polling_source(country: str) -> list[str]:
    """Return urls for the polling source. Currently uses Wikipedia pages mapping."""
    if country in WIKI_POLLING_PAGES:
        return WIKI_POLLING_PAGES[country].get("links", [])
    return []


def get_polling_headers(country: str) -> list[str] | None:
    """Return the list of headers for the given country."""
    if country in WIKI_POLLING_PAGES:
        return WIKI_POLLING_PAGES[country].get("headers")
    return None
