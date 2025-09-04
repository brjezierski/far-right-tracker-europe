from __future__ import annotations
from io import StringIO
import re
from typing import Dict, List, Optional, Tuple
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from datetime import datetime
from pathlib import Path
from .utils import DATA_DIR, save_json
import urllib.parse
import json

# Load polling sources from JSON file
_SOURCES_PATH = Path(__file__).resolve().parent / "polling_sources.json"
try:
    DEFAULT_WIKI_POLLING_PAGES: Dict[str, str] = json.loads(
        _SOURCES_PATH.read_text(encoding="utf-8")
    )
except Exception:
    DEFAULT_WIKI_POLLING_PAGES = {}

COUNTRY_TABLE_HEADERS = {"Ireland": ["National polls"]}
HISTORICAL_SEPERATORS = ["Formerly:", "Historical"]


def normalize_party_name(s: str) -> str:
    return re.sub(r"\W+", "", s.lower()).replace("_", "").replace("party", "")


def parse_date(s: str) -> Optional[pd.Timestamp]:
    # parse date ranges as well, e.g. "1–2 January 2024" or "1/2 January 2024" by extracting the last part
    s = s.strip()
    if "–" in s or "/" in s:
        # Split by the last occurrence of '–' or '/' and take the last part
        parts = re.split(r"[–/]", s)
        s = parts[-1].strip()

    try:
        return pd.Timestamp(dateparser.parse(s, dayfirst=True))
    except Exception:
        return None


class WikipediaPollingFetcher:
    def __init__(self, url: str):
        self.url = url

    def fetch_tables(self, country: str) -> List[pd.DataFrame]:
        """Prefer the first table under the section whose heading equals the current year.
        If not found, try previous year. Fallback to all tables on the page.
        """
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": "http://google.com",
            }

            r = requests.get(self.url, timeout=30, headers=headers)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            years = [str(datetime.utcnow().year), str(datetime.utcnow().year - 1)]
            specific_headers = (
                COUNTRY_TABLE_HEADERS[country]
                if country in COUNTRY_TABLE_HEADERS
                else years
            )

            for specific_header in specific_headers:
                # Look for heading elements with text equal to the year
                for header in soup.find_all(["h1", "h2", "h3", "h4"]):
                    span = header.find("span", class_="mw-headline")
                    text = (
                        span.get_text(strip=True)
                        if span
                        else header.get_text(strip=True)
                    )
                    if text != specific_header:
                        continue
                    # Find the first table following this header, before the next heading at the same or higher level
                    node = header

                    while True:
                        node = node.find_next_sibling()
                        if node is None:
                            break
                        if node.name in ["h1", "h2", "h3", "h4"]:
                            break
                        if node.name == "table":
                            try:
                                dfs = pd.read_html(str(node), extract_links="header")
                                if dfs:
                                    return [dfs[0]]
                            except Exception:
                                pass
                            break

            # Fallback: return all tables
            print("No specific year tables found, returning all tables.")
            return pd.read_html(StringIO(str(r.text)), extract_links="header")
        except Exception as e:
            # Last resort: try pandas directly, else empty
            try:
                print(f"Error fetching specific year tables, returning all tables. {e}")
                return pd.read_html(self.url, extract_links="header")
            except Exception:
                return []

    def fetch_latest_and_series(
        self, country: str, categories: List[str]
    ) -> Tuple[Optional[float], Dict[str, List[Dict]]]:
        """
        Returns tuple of (latest_total_support, series_by_party)
        series_by_party: {party: [{date, value}]}
        Also annotates party political positions into data/all_parties.json using header links when available.
        """
        try:
            tables = self.fetch_tables(country)
            print(f"Fetched {len(tables)} tables for {country} from {self.url}")
        except Exception:
            return None, {}
        if len(tables) == 0:
            print(f"No tables found for {country} at {self.url}")
            return None, {}

        series: Dict[str, List[Dict]] = {}
        latest_values: Dict[str, float] = {}

        def split_header_to_text_link(col) -> Tuple[str, str]:
            # Use last level if MultiIndex; expects tuples like (text, href) when extract_links='header'
            first = col[0] if isinstance(col, tuple) else col
            last = col[-1] if isinstance(col, tuple) else col
            text: str
            href: str = ""
            if isinstance(first, tuple):
                # Common case: (text, href)
                if len(first) >= 2 and isinstance(first[0], str):
                    text = first[0]
                    if len(first) >= 2 and isinstance(first[1], str):
                        href = first[1]
                    elif len(last) >= 2 and isinstance(last[1], str):
                        href = last[1]
                    else:
                        href = ""
                else:
                    text = " ".join(str(x) for x in first if isinstance(x, str))
            else:
                text = str(first)
            return urllib.parse.unquote(text), urllib.parse.unquote(href or "")

        # Heuristic: find the first table that looks like a poll list (has a date column and multiple party columns)
        for df in tables:
            # Build parallel arrays of header display texts and links
            cols: List[str] = []
            links: List[str] = []
            for col in df.columns:
                header_text, header_link = split_header_to_text_link(col)
                column_name = (
                    header_link.split("/")[-1]
                    if header_link
                    else header_text.lower().strip()
                )
                cols.append(column_name)
                links.append(header_link)
            # Assign normalized text headers to DataFrame columns
            df.columns = cols
            if not any(
                "date" in c or "fieldwork" in c or "conducted" in c for c in cols
            ):
                continue

            header_parties = [
                {"name": link.split("/")[-1], "link": link}
                for link in links
                if len(link) > 0
            ]

            far_right_parties = annotate_parties_positions(
                country, header_parties, categories
            )
            if not far_right_parties:
                continue

            # Extract date column
            date_col = next(
                (
                    c
                    for c in cols
                    if "date" in c or "fieldwork" in c or "conducted" in c
                ),
                None,
            )
            if not date_col:
                continue

            # Iterate rows
            for _, row in df.iterrows():
                date_raw = str(row.get(date_col, "")).strip()
                date = parse_date(date_raw)

                if date is None:
                    continue
                for party in far_right_parties:
                    try:
                        val = row.get(party)
                        # if is of type pandas series, convert to string
                        # Values may be strings like '23%' or '23.5'
                        if isinstance(val, str):
                            val = (
                                val.replace("%", "")
                                .replace(",", ".")
                                .strip()
                                .split()[0]
                            )
                            v = float(val)
                        elif isinstance(val, (int, float)):
                            v = float(val)
                        else:
                            try:
                                v = 0
                                for i in range(len(val)):
                                    if isinstance(val.iloc[i], str):
                                        val_i = val.iloc[i]
                                        val_i = (
                                            val_i.replace("%", "")
                                            .replace(",", ".")
                                            .strip()
                                            .split()[0]
                                        )
                                        val_i = float(val_i)
                                        # sometimes multiple parties in a coalition have the support listed together multiple times
                                        # here we apply a heuristic: if the same polling value appears we assume the case is as above,
                                        # otherwise the support is listed for coalition parties separately
                                        if val_i != v:
                                            v += val_i
                            except Exception as e:
                                print(
                                    f"Error converting Series to string for {party} in {country}: {e}"
                                )

                        if party not in series:
                            series[party] = []

                        if pd.isna(v) or v > 100 or v < 0:
                            continue

                        series[party].append(
                            {"date": date.date().isoformat(), "value": v}
                        )
                    except Exception:
                        continue

            # Latest per party: take max by date
            for party, pts in series.items():
                if pts:
                    latest = max(pts, key=lambda x: x["date"])  # type: ignore
                    latest_values[party] = latest["value"]
            # If we got some values, break
            if latest_values:
                break

        latest_total = sum(latest_values.values()) if latest_values else None
        return latest_total, series


def get_best_polling_source(
    country: str, override_url: Optional[str] = None
) -> Tuple[str, Optional[str]]:
    """Return (source_type, url) for best polling source. Currently uses Wikipedia pages mapping.
    Future: Prefer Politico Poll of Polls if stable JSON endpoint is available.
    """
    if override_url:
        return ("wikipedia", override_url)
    if country in DEFAULT_WIKI_POLLING_PAGES:
        return ("wikipedia", DEFAULT_WIKI_POLLING_PAGES[country])
    return ("wikipedia", None)


def _load_json(path: Path) -> dict:
    if path.exists():
        try:
            return pd.io.json.loads(path.read_text(encoding="utf-8"))  # type: ignore
        except Exception:
            try:
                import json

                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                return {}
    return {}


def _clean_text(s: str) -> str:
    # Remove references like [1] or [ 1 ] or  [A] or [ A ]
    s = re.sub(r"\[\s*\w+\s*\]", "", s)
    s = re.sub(r"\[\s*\d+\s*\]", "", s)
    # use HISTORICAL_SEPERATORS
    for sep in HISTORICAL_SEPERATORS:
        if sep in s:
            print(f"Removing historical separator '{sep}' from text: {s}")
            s = s.split(sep, 1)[0].strip()
    return re.sub(r"\s+", " ", s).strip()


def _fetch_political_position(url: str) -> Optional[dict]:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "http://google.com",
        }
        r = requests.get(url, timeout=30, headers=headers)
        r.raise_for_status()
    except Exception:
        return None
    soup = BeautifulSoup(r.text, "lxml")
    # retrieve the following table <table class="infobox vcard">
    table = soup.select_one("table.infobox.vcard") or soup.select_one("table.infobox")
    if not table:
        print(f"No infobox found in {url}")
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
            political_position = _clean_text(text)
            print(
                f"Found political position for {label}: {political_position} in {url}"
            )

        if label.startswith("ideology"):
            td = row.find("td")
            if not td:
                return None
            text = td.get_text(" ", strip=True)
            ideology = _clean_text(text)
            print(f"Found ideology for {label}: {ideology} in {url}")

    if political_position or ideology:
        return {
            "political_position": political_position,
            "ideology": ideology,
            "url": url,
        }
    return None


def annotate_parties_positions(
    country: str, parties: List[Dict[str, str]], categories: List[str]
) -> List[str]:
    """
    For a given country (name), and a list of party dicts containing at least 'name' and optionally 'link',
    fetch the 'Political position' from each party's Wikipedia infobox (infobox vcard).
    Cache results in data/all_parties.json so subsequent runs only fetch missing entries.
    Returns a list of party names whose political position is exactly 'Far-right' (case-insensitive, hyphen/space tolerant).
    Also updates data/all_parties.json with all positions.
    """
    cache_path = DATA_DIR / "all_parties.json"
    cache = _load_json(cache_path)
    if "countries" not in cache:
        cache["countries"] = {}
    if country and country not in cache["countries"]:
        cache["countries"][country] = {}

    country_cache: Dict[str, Dict[str, str]] = (
        cache["countries"].get(country, {}) if country else {}
    )

    far_right: List[str] = []
    for p in parties:
        name = str(p.get("name") or "").strip()
        link = (p.get("link") or "").strip()
        if not name:
            continue
        party_description = country_cache.get(name)

        if not party_description:
            # Build URL if link provided
            url = None
            if link:
                if link.startswith("http://") or link.startswith("https://"):
                    url = link
                else:
                    # ensure single slash between host and path
                    url = f"https://en.wikipedia.org/{link.lstrip('/')}"
            if url:
                party_description = _fetch_political_position(url)
                if party_description:
                    country_cache[name] = party_description

        # Determine if exactly Far-right
        if party_description:
            political_position = party_description.get("political_position")
            if political_position:
                political_position = (
                    political_position.strip().lower().replace(" ", "-")
                )
            else:
                political_position = ""
            ideology = party_description.get("ideology")
            if ideology:
                ideology = ideology.strip().lower().replace(" ", "-")
            else:
                ideology = ""
            for category in categories:
                if category in political_position or category in ideology:
                    far_right.append(name)

    if country:
        cache["countries"][country] = country_cache
    cache["updatedAt"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    try:
        save_json(cache_path, cache)  # type: ignore[arg-type]
    except Exception:
        # fallback save
        try:
            import json

            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(
                json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass

    return far_right


def update_summary_with_far_right(summary: dict) -> None:
    summary_path = DATA_DIR / "summary.json"
    summary["updatedAt"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    save_json(summary_path, summary)  # type: ignore[arg-type]
