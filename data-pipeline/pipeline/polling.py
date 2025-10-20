from __future__ import annotations
from io import StringIO
import re
from typing import Dict, List, Optional, Tuple
import requests
import pandas as pd
from bs4 import BeautifulSoup

from datetime import datetime
from pathlib import Path
from .utils import DATA_DIR, save_json
import urllib.parse
import json
from dateparser import parse

# Load polling sources from JSON file
_SOURCES_PATH = Path(__file__).resolve().parent / "polling_sources.json"
try:
    WIKI_POLLING_PAGES: Dict[str, str] = json.loads(
        _SOURCES_PATH.read_text(encoding="utf-8")
    )
except Exception:
    WIKI_POLLING_PAGES = {}

COUNTRY_TABLE_HEADERS = {
    "Albania": ["Nationwide"],
    "Armenia": ["Opinion polls"],
    "Czech Republic": ["Polls"],
    "Denmark": ["Opinion polls"],
    "Hungary": ["Polling"],
    "Ireland": ["National polls"],
    "Italy": ["Party vote"],
    "Kosovo": ["National polls"],
    "Latvia": ["Opinion polls"],
    "Luxembourg": ["Voting intention"],
    "Malta": ["Expressing a Preference"],
    "Norway": ["National poll results"],
    "Poland": ["Poll results"],
    "Portugal": ["Nationwide polling"],
    "Romania": ["Party polls"],
    "Russia": ["Pre-campaign polls"],
    "Serbia": ["Poll results"],
    "Slovakia": ["Electoral polling"],
    "Sweden": ["Opinion polls"],
    "Switzerland": ["Nationwide polling"],
    "Turkey": ["Party vote"],
    "Ukraine": ["Poll results"],
    "United Kingdom": ["National poll results"],
}
COUNTRY_EXCLUDED_HEADERS = {
    "Denmark": ["Constituency polling"],
}
HISTORICAL_SEPERATORS = ["Formerly:", "Historical"]


def normalize_party_name(s: str) -> str:
    return re.sub(r"\W+", "", s.lower()).replace("_", "").replace("party", "")


def parse_date(
    s: str,
    prev_date: Optional[pd.Timestamp] = None,
    prev_date_year_given: bool = False,
    table_header_key: str = "",
) -> Tuple[Optional[pd.Timestamp], bool]:
    # parse date ranges as well, e.g. "1–2 January 2024" or "1/2 January 2024" by extracting the last part
    s = s.strip()
    if "–" in s or "/" in s or "-" in s:
        # Split by the last occurrence of '–' or '/' and take the last part
        parts = re.split(r"[–/-]", s)
        s = parts[-1].strip()

    # if table_header_key is a year, and s does not contain a 4-digit year, then append the year to s
    if (
        table_header_key.isdigit()
        and len(table_header_key) == 4
        and not re.search(r"\b\d{4}\b", s)
    ):
        s = f"{s} {table_header_key}"

    settings = {"DATE_ORDER": "DMY"}
    if prev_date:
        settings["RELATIVE_BASE"] = prev_date.to_pydatetime()  # type: ignore # .strftime("%Y-%m-%d")
    try:
        parsed_date = parse(s, settings=settings)  # type: ignore
        if parsed_date:
            # if parsed_date is before the prev_date, and the year of parsed_date is not in s, then assume the year is next year
            if (
                prev_date
                and parsed_date < prev_date.to_pydatetime()
                and str(prev_date.year) not in s
                and prev_date_year_given
            ):
                parsed_date = parsed_date.replace(year=parsed_date.year + 1)
            # if s contains a 4-digit year, then we assume the year is given
            prev_date_year_given = bool(re.search(r"\b\d{4}\b", s))
            return pd.Timestamp(parsed_date), prev_date_year_given
        else:
            return None, False

    except Exception:
        return None, False


class WikipediaPollingFetcher:
    def __init__(self, url: str):
        self.url = url

    def fetch_tables(self, country: str) -> Dict[str, List[pd.DataFrame]]:
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
            excluded_headers = (
                COUNTRY_EXCLUDED_HEADERS[country]
                if country in COUNTRY_EXCLUDED_HEADERS
                else []
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
                    header_level = int(header.name[1])
                    possible_headers = ["h1", "h2", "h3", "h4"]
                    stopping_headers = possible_headers[:header_level]

                    df_dict = {}
                    node_header_text = ""
                    while True:
                        # get child node
                        node = node.find_next()
                        # get all headers from node
                        headers_in_node = (
                            [h.name for h in node.find_all(possible_headers)]
                            if node
                            else []
                        )
                        if len(headers_in_node) > 0:
                            for header_in_node in headers_in_node:
                                # get span and text
                                span_in_node = node.find("span", class_="mw-headline")
                                text_in_node = (
                                    span_in_node.get_text(strip=True)
                                    if span_in_node
                                    else node.get_text(strip=True)
                                )
                                # remove [edit] from text
                                node_header_text = re.sub(
                                    r"\[edit\]$", "", text_in_node
                                ).strip()

                        if node is None or node.name in stopping_headers:
                            if len(df_dict) > 0:
                                return df_dict
                            break
                        if node.name == "table":
                            try:
                                dfs = pd.read_html(
                                    StringIO(str(node)), extract_links="header"
                                )
                                if node_header_text not in df_dict:
                                    df_dict[node_header_text] = dfs
                                else:
                                    df_dict[node_header_text].extend(dfs)
                                # if dfs:
                                #     return [dfs[0]]
                            except Exception:
                                if len(df_dict) > 0:
                                    return df_dict
                                pass
                            # break

                    if len(df_dict) > 0:
                        return df_dict
            # Fallback: return all tables
            print("No specific year tables found, returning all tables.")
            return {"": pd.read_html(StringIO(str(r.text)), extract_links="header")}
        except Exception as e:
            # Last resort: try pandas directly, else empty
            try:
                print(f"Error fetching specific year tables, returning all tables. {e}")
                return {"": pd.read_html(self.url, extract_links="header")}
            except Exception:
                return {}

    def fetch_latest_and_series(
        self, country: str, categories: List[str]
    ) -> Tuple[Optional[float], Dict[str, List[Dict]], Dict[str, Dict]]:
        """
        Returns tuple of (latest_total_support, series_by_party, party_metadata)
        series_by_party: {party: [{date, value}]}
        party_metadata: {party: {political_position, ideology, url, is_far_right}}
        Also annotates party political positions into data/all_parties.json using header links when available.
        """
        try:
            tables_dict = self.fetch_tables(country)
            print(f"Fetched {len(tables_dict)} tables for {country} from {self.url}")
        except Exception:
            return None, {}, {}
        if len(tables_dict) == 0:
            print(f"No tables found for {country} at {self.url}")
            return None, {}, {}

        series: Dict[str, List[Dict]] = {}
        latest_values: Dict[str, float] = {}
        party_metadata: Dict[str, Dict] = {}

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
                        text += " | " + last[0]
                    elif isinstance(last[0], str):
                        href = ""
                        text += " | " + last[0]
                    else:
                        href = ""
                else:
                    text = " ".join(str(x) for x in first if isinstance(x, str))
            else:
                text = str(first)
            return urllib.parse.unquote(text), urllib.parse.unquote(href or "")

        # Heuristic: find the first table that looks like a poll list (has a date column and multiple party columns)
        for tables_key in tables_dict:
            for df in tables_dict[tables_key]:
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

                # No date column found, skipping table
                if not any(
                    "date" in c or "fieldwork" in c or "conducted" in c for c in cols
                ):
                    continue

                header_parties = [
                    {"name": link.split("/")[-1], "link": link}
                    for link in links
                    if len(link) > 0
                ]
                header_parties_links = [
                    header_party["link"] for header_party in header_parties
                ]

                # Get all party information and identify selected parties
                all_parties_with_positions = annotate_parties_positions(
                    country, header_parties, categories
                )
                selected_parties = all_parties_with_positions["selected_parties"]
                all_party_metadata = all_parties_with_positions["party_metadata"]

                # Process all parties that have polling data (not just selected)
                potential_party_columns = [
                    col
                    for col, link in zip(cols, links)
                    if link in header_parties_links
                ]

                if not potential_party_columns:
                    continue

                # Extract date column
                # TODO make it into a function
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
                pollster_col_ind = 0

                # Iterate rows from the bottom
                prev_date: Optional[pd.Timestamp] = None
                prev_date_year_given: bool = False
                for i in range(len(df) - 1, -1, -1):
                    row = df.iloc[i]
                    date_raw = str(row.get(date_col, "")).strip()
                    pollster = str(row.iloc[pollster_col_ind]).strip()
                    if "election" in pollster.lower():
                        continue
                    date, prev_date_year_given = parse_date(
                        date_raw, prev_date, prev_date_year_given, tables_key
                    )
                    if date:
                        prev_date = date

                    if date is None:
                        continue

                    for party in potential_party_columns:
                        try:
                            val = row.get(party)
                            if val == "-":
                                continue
                            v = None
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
                                    if val is not None and hasattr(val, "iloc"):
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
                                    continue

                            if party not in series:
                                series[party] = []

                            if v is None or pd.isna(v) or v > 100 or v < 0:
                                continue

                            series[party].append(
                                {"date": date.date().isoformat(), "value": v}
                            )

                            # Store party metadata
                            if party not in party_metadata:
                                party_metadata[party] = all_party_metadata.get(
                                    party,
                                    {
                                        "political_position": None,
                                        "ideology": None,
                                        "url": None,
                                        "is_far_right": party in selected_parties,
                                    },
                                )
                        except Exception:
                            continue

                # Latest per party: take max by date
                for party, pts in series.items():
                    if pts:
                        latest = max(pts, key=lambda x: x["date"])  # type: ignore
                        latest_values[party] = latest["value"]
                # # If we got some values, break
                # if latest_values:
                #     break

        latest_total = sum(latest_values.values()) if latest_values else None
        return latest_total, series, party_metadata


def get_polling_source(country: str) -> Tuple[str, Optional[str]]:
    """Return (source_type, url) for the polling source. Currently uses Wikipedia pages mapping."""
    if country in WIKI_POLLING_PAGES:
        return ("wikipedia", WIKI_POLLING_PAGES[country])
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

        if label.startswith("ideology"):
            td = row.find("td")
            if not td:
                return None
            text = td.get_text(" ", strip=True)
            ideology = _clean_text(text)

    if political_position or ideology:
        return {
            "political_position": political_position,
            "ideology": ideology,
            "url": url,
        }
    return None


def annotate_parties_positions(
    country: str, parties: List[Dict[str, str]], categories: List[str]
) -> Dict:
    """
    For a given country (name), and a list of party dicts containing at least 'name' and optionally 'link',
    fetch the 'Political position' from each party's Wikipedia infobox (infobox vcard).
    Cache results in data/all_parties.json so subsequent runs only fetch missing entries.
    Returns a dict with:
    - 'selected_parties': list of party names whose political position matches categories
    - 'party_metadata': dict of {party_name: {political_position, ideology, url, is_far_right}}
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
    party_metadata: Dict[str, Dict] = {}

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

        # Store party metadata
        is_far_right = False
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

            # Check if selected
            for category in categories:
                if category in political_position or category in ideology:
                    far_right.append(name)
                    is_far_right = True
                    break

            party_metadata[name] = {
                "political_position": party_description.get("political_position"),
                "ideology": party_description.get("ideology"),
                "url": party_description.get("url"),
                "is_far_right": is_far_right,
            }
        else:
            party_metadata[name] = {
                "political_position": None,
                "ideology": None,
                "url": None,
                "is_far_right": False,
            }

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

    return {"selected_parties": far_right, "party_metadata": party_metadata}
