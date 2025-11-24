from __future__ import annotations
import re
from typing import Dict, List, Optional, Tuple
import requests
import pandas as pd
from bs4 import BeautifulSoup

from datetime import datetime
from pathlib import Path
from .utils import (
    DATA_DIR,
    save_json,
    get_polling_value,
    get_latest_polling_value,
    parse_html_table_with_hierarchy,
    extract_value_from_hierarchical_tuple,
    parse_all_tables_from_soup,
    find_date_column,
    extract_hierarchical_value_by_level,
    extract_party_name_from_link,
)
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
    "Belgium": ["Federal"],
    "Czech Republic": ["Polls"],
    "Denmark": ["Opinion polls"],
    "France": ["Opinion polling"],
    "Hungary": ["Polling"],
    "Iceland": ["Opinion polls"],
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
DEBUG = False


def normalize_party_name(s: str) -> str:
    return re.sub(r"\W+", "", s.lower()).replace("_", "").replace("party", "")


def parse_date(
    s: str | tuple,
    prev_date: Optional[pd.Timestamp] = None,
    prev_date_year_given: bool = False,
    table_header_key: str = "",
) -> Tuple[Optional[pd.Timestamp], bool]:
    # parse date ranges as well, e.g. "1–2 January 2024" or "1/2 January 2024" by extracting the last part
    if isinstance(s, tuple):
        for item in s:
            if isinstance(item, str):
                s = item
                break

    s = s.strip()  # type: ignore
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
            # if parsed_date is after today return None
            if parsed_date > datetime.utcnow():
                return None, prev_date_year_given
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
            years = []
            # get years between current year and current year -10
            for y in range(datetime.utcnow().year, datetime.utcnow().year - 10, -1):
                years.append(str(y))
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
            possible_headers = ["h1", "h2", "h3", "h4", "h5"]

            for specific_header in specific_headers:
                # Look for heading elements with text equal to the year
                for header in soup.find_all(possible_headers):
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
                                # Use custom parser to preserve hierarchy
                                df = parse_html_table_with_hierarchy(str(node))
                                dfs = [df] if not df.empty else []

                                # save node as html for debugging
                                if DEBUG:
                                    with open(
                                        f"debug_{country}_{specific_header}_{node_header_text}.html",
                                        "w",
                                        encoding="utf-8",
                                    ) as f:
                                        f.write(str(node))
                                if node_header_text not in df_dict:
                                    df_dict[node_header_text] = dfs
                                else:
                                    df_dict[node_header_text].extend(dfs)
                            except Exception as e:
                                print(f"Error parsing table: {e}")
                                if len(df_dict) > 0:
                                    return df_dict
                                pass

                    if len(df_dict) > 0:
                        return df_dict
            # Fallback: return all tables
            print("No specific year tables found, returning all tables.")
            # Parse all tables with custom parser
            fallback_dfs = parse_all_tables_from_soup(soup)
            return {"": fallback_dfs}
        except Exception as e:
            # Last resort: try custom parser on all tables
            try:
                print(f"Error fetching specific year tables, returning all tables. {e}")
                r = requests.get(self.url, timeout=30)
                soup = BeautifulSoup(r.text, "lxml")
                fallback_dfs = parse_all_tables_from_soup(soup)
                return {"": fallback_dfs}
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
        latest_total: Optional[float] = None

        # Heuristic: find the first table that looks like a poll list (has a date column and multiple party columns)
        for tables_key in tables_dict:
            for df in tables_dict[tables_key]:
                # save df locally for debugging
                if DEBUG:
                    df.to_csv(f"debug_{country}_{tables_key}.csv", index=False)

                # Process hierarchical column structure
                cols_info = process_hierarchical_columns(df)

                # Check for date column
                date_col_info = find_date_column(cols_info)

                if not date_col_info:
                    print(f"No date column found in table for {country}")
                    continue

                # Extract party columns (those with links) and fix empty names
                header_parties = []
                for col_info in cols_info:
                    if col_info["column_link"]:
                        column_name = col_info["column_name"]
                        # If name is empty, extract it from the link
                        if not column_name or column_name.strip() == "":
                            column_name = extract_party_name_from_link(
                                col_info["column_link"]
                            )
                        # Clean party name (remove references like [b])
                        column_name = _clean_text(column_name)
                        # Update the column_name in cols_info so it's used later
                        col_info["column_name"] = column_name
                        header_parties.append(
                            {"name": column_name, "link": col_info["column_link"]}
                        )

                # Get all party information and identify selected parties
                all_parties_with_positions = annotate_parties_positions(
                    country, header_parties, categories
                )
                selected_parties = all_parties_with_positions["selected_parties"]
                all_party_metadata = all_parties_with_positions["party_metadata"]

                # Process all parties that have polling data
                potential_party_cols = [
                    col_info
                    for col_info in cols_info
                    if col_info["column_link"] and col_info != date_col_info
                ]

                if not potential_party_cols:
                    continue

                # Get pollster column (first column, typically)
                pollster_col = cols_info[0]["original"] if cols_info else None

                # Iterate rows from the bottom
                prev_date: Optional[pd.Timestamp] = None
                prev_date_year_given: bool = False
                for i in range(len(df) - 1, -1, -1):
                    row = df.iloc[i]

                    # Extract date from date column
                    date_raw = row[date_col_info["original"]]
                    date_str = extract_value_from_hierarchical_tuple(date_raw)

                    # Skip rows with empty date cells (continuation rows from rowspan)
                    if not date_str or date_str.strip() == "":
                        continue

                    # Extract pollster
                    pollster_raw = row[pollster_col] if pollster_col else ""
                    pollster = extract_value_from_hierarchical_tuple(pollster_raw, "")

                    # Skip election results
                    if "election" in pollster.lower():
                        continue

                    date, prev_date_year_given = parse_date(
                        date_str, prev_date, prev_date_year_given, tables_key
                    )
                    if DEBUG:
                        print(f"Parsing date string: '{date_str}' into date: {date}")
                    if date:
                        prev_date = date

                    if date is None:
                        continue

                    for party_col_info in potential_party_cols:
                        try:
                            party_name = party_col_info["column_name"]
                            is_parent = party_col_info.get("is_parent", False)
                            is_hierarchical = party_col_info.get(
                                "is_hierarchical", False
                            )
                            polling_value_raw = row[party_col_info["original"]]

                            # Extract polling value from tuple structure based on parent/child level
                            polling_value_str = extract_hierarchical_value_by_level(
                                polling_value_raw, is_parent, is_hierarchical, ""
                            )

                            if not polling_value_str or polling_value_str in [
                                "",
                                "nan",
                                "None",
                            ]:
                                continue

                            polling_value = get_polling_value(
                                polling_value_str, party_name, country
                            )

                            if (
                                pd.isna(polling_value)
                                or polling_value > 100
                                or polling_value < 0
                            ):
                                continue

                            if party_name not in series:
                                series[party_name] = []
                            series[party_name].append(
                                {
                                    "date": date.date().isoformat(),
                                    "value": polling_value,
                                }
                            )

                            # Store party metadata
                            if party_name not in party_metadata:
                                party_metadata[party_name] = all_party_metadata.get(
                                    party_name,
                                    {
                                        "political_position": None,
                                        "ideology": None,
                                        "url": None,
                                        "is_far_right": party_name in selected_parties,
                                    },
                                )
                        except Exception as e:
                            continue

        # Calculate latest total support using the extracted method
        latest_total = calculate_latest_total_support(series, party_metadata)

        # Also calculate latest per party for other uses
        for party, pts in series.items():
            if pts:
                latest_values[party] = get_latest_polling_value(pts)

        return latest_total, series, party_metadata


def process_hierarchical_columns(df: pd.DataFrame) -> List[Dict]:
    """
    Process hierarchical column structure from a DataFrame.
    Columns are tuples: ((parent_text, parent_link), (child_text, child_link))

    Args:
        df: DataFrame with hierarchical column headers

    Returns:
        List of column info dictionaries containing original column, column names, links, etc.
    """
    cols_info = []
    for col in df.columns:
        if isinstance(col, tuple) and len(col) == 2:
            parent_info, child_info = col
            parent_text, parent_link = (
                parent_info
                if isinstance(parent_info, tuple)
                else (str(parent_info), None)
            )
            child_text, child_link = (
                child_info
                if isinstance(child_info, tuple) and child_info != ()
                else ("", None)
            )

            # For hierarchical columns, create entries for both parent and child
            if child_text:
                # If there's child text, use it as the primary column name
                # regardless of whether there's a link
                cols_info.append(
                    {
                        "original": col,
                        "column_name": child_text,
                        "column_link": child_link or "",
                        "parent_text": parent_text,
                        "parent_link": parent_link,
                        "child_text": child_text,
                        "is_hierarchical": True,
                        "is_parent": False,
                    }
                )
                # Also create parent column entry if parent has link
                if parent_link and child_link:
                    cols_info.append(
                        {
                            "original": col,
                            "column_name": parent_text,
                            "column_link": parent_link,
                            "parent_text": parent_text,
                            "parent_link": parent_link,
                            "child_text": child_text,
                            "is_hierarchical": True,
                            "is_parent": True,
                        }
                    )
            else:
                # Non-hierarchical column with just parent info
                cols_info.append(
                    {
                        "original": col,
                        "column_name": parent_text,
                        "column_link": parent_link or "",
                        "parent_text": parent_text,
                        "parent_link": parent_link,
                        "child_text": "",
                        "is_hierarchical": False,
                        "is_parent": False,
                    }
                )
        else:
            # Fallback for non-tuple columns
            cols_info.append(
                {
                    "original": col,
                    "column_name": str(col).lower().strip(),
                    "column_link": "",
                    "parent_text": str(col),
                    "parent_link": None,
                    "child_text": "",
                    "is_hierarchical": False,
                    "is_parent": False,
                }
            )

    return cols_info


def calculate_latest_total_support(
    series: Dict[str, List[Dict]], party_metadata: Dict[str, Dict]
) -> Optional[float]:
    """
    Calculate latest total support as the average of total support from the latest 3 polls.
    Only includes parties marked as far-right (is_far_right=True).

    Args:
        series: {party: [{date, value}]}
        party_metadata: {party: {is_far_right, ...}}

    Returns:
        Average of sum of far-right party support across latest 3 poll dates, or None if no data
    """
    # Get all unique dates
    all_dates_set = set()
    for party_name, pts in series.items():
        for pt in pts:
            all_dates_set.add(pt["date"])

    if not all_dates_set:
        return None

    # Sort dates in descending order (most recent first)
    sorted_dates = sorted(list(all_dates_set), reverse=True)
    latest_3_dates = sorted_dates[:3]

    # Calculate total support for each of the latest 3 dates (only for far-right parties)
    totals_per_date = []
    for date_str in latest_3_dates:
        date_total = 0
        for party_name, pts in series.items():
            # Only include parties that are marked as far-right
            if party_name in party_metadata and party_metadata[party_name].get(
                "is_far_right", False
            ):
                # Find the polling value for this party on this date
                for pt in pts:
                    if pt["date"] == date_str:
                        date_total += pt["value"]
                        break
        totals_per_date.append(date_total)

    # Average the totals
    return sum(totals_per_date) / len(totals_per_date) if totals_per_date else None


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
