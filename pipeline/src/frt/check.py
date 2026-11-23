"""Health check for polling sources: ``frt check [--max-age-days N]``.

Exits non-zero when a source link fails, redirects to another page, or a
country has no polls or only polls older than the threshold.
"""

from __future__ import annotations

import csv
import logging
from datetime import date, timedelta
from urllib.parse import unquote, urlparse

import requests

from frt.config import COUNTRIES_DIR
from frt.countries import get_country_iso_code
from frt.scraping.sources import WIKI_POLLING_PAGES

logger = logging.getLogger(__name__)

USER_AGENT = "FarRightTrackerBot/1.0 (https://github.com/brjezierski/far-right-tracker-europe)"


def check_link(url: str) -> str | None:
    """Return a problem description for the url, or None if it resolves to the same page."""
    try:
        r = requests.head(url, timeout=30, allow_redirects=True, headers={"User-Agent": USER_AGENT})
    except requests.RequestException as e:
        return f"unreachable ({e.__class__.__name__})"
    if r.status_code >= 400:
        return f"HTTP {r.status_code}"
    if unquote(urlparse(r.url).path) != unquote(urlparse(url).path):
        return f"redirects to {r.url}"
    return None


def latest_poll_date(iso2: str) -> date | None:
    """Return the most recent poll date in the country's CSV, if any."""
    path = COUNTRIES_DIR / iso2 / "polling_data.csv"
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as f:
        dates = [row["date"] for row in csv.DictReader(f) if row.get("date")]
    return date.fromisoformat(max(dates)) if dates else None


def check_sources(max_age_days: int, selected_country: str | None = None) -> list[str]:
    """Return one message per problem found; an empty list means all sources look healthy."""
    problems = []
    cutoff = date.today() - timedelta(days=max_age_days)
    for country, source in WIKI_POLLING_PAGES.items():
        if selected_country and country != selected_country:
            continue
        for url in source.get("links", []):
            if problem := check_link(url):
                problems.append(f"{country}: {url}: {problem}")
        latest = latest_poll_date(get_country_iso_code(country))
        if latest is None:
            problems.append(f"{country}: no polls")
        elif latest < cutoff:
            problems.append(f"{country}: last poll {latest} is older than {max_age_days} days")
    return problems


def run(max_age_days: int, selected_country: str | None = None) -> int:
    problems = check_sources(max_age_days, selected_country)
    for problem in problems:
        logger.error(problem)
    logger.info(f"{len(problems)} problem(s) found.")
    return 1 if problems else 0
