from __future__ import annotations
import re
from typing import Dict, List, Optional, Tuple
import requests
import pandas as pd
from bs4 import BeautifulSoup

from datetime import datetime
from pathlib import Path
from .utils import (
    COUNTRIES_DIR,
    get_country_iso_code,
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
    WIKI_POLLING_PAGES: Dict[str, Dict] = json.loads(
        _SOURCES_PATH.read_text(encoding="utf-8")
    )
except Exception:
    WIKI_POLLING_PAGES = {}

COUNTRY_EXCLUDED_HEADERS = {
    "Denmark": ["Constituency polling"],
    "Poland": ["Alternative scenarios", "Parties"],
}
HISTORICAL_SEPERATORS = ["Formerly:", "Historical"]
DEBUG = False


def set_debug_mode(enabled: bool):
    """Enable or disable debug mode."""
    global DEBUG
    DEBUG = enabled


def normalize_party_name(s: str) -> str:
    return re.sub(r"\W+", "", s.lower()).replace("_", "").replace("party", "")


def parse_date(
    s: str | tuple,
    url: str,
    prev_date: Optional[pd.Timestamp] = None,
    prev_date_year_given: bool = False,
    table_header_key: str = "",
) -> Tuple[Optional[pd.Timestamp], bool]:
    # get year from url if possible
    # e.g. "https://en.wikipedia.org/wiki/2024_Slovak_parliamentary_election" or "https://en.wikipedia.org/wiki/Opinion_polling_for_the_September_2015_Greek_parliamentary_election"
    # extract 4 digit year from anywhere in url
    election_year = re.search(r"(\d{4})", url)
    if election_year:
        cutoff_date = datetime(int(election_year.group(1)), 12, 31)
    else:
        cutoff_date = datetime.utcnow()
    # parse date ranges as well, e.g. "1–2 January 2024" or "1/2 January 2024" by extracting the last part
    if isinstance(s, tuple):
        for item in s:
            if isinstance(item, str):
                s = item
                break

    s = s.strip().replace("-", "–")  # type: ignore
    # if s has format dddd-dd-dd read as yyyy-mm-dd
    if re.fullmatch(r"\d{4}–\d{2}–\d{2}", s):
        year, month, day = map(int, s.split("–"))
        return pd.Timestamp(year=year, month=month, day=day), True

    if ("–" in s or "/" in s) and s.count("–") != 2 and s.count("/") != 2:
        # Split by the last occurrence of '–' or '/' and take the last part
        parts = re.split(r"[–/]", s)
        s = parts[-1].strip()

    if not isinstance(s, str):
        return None, False

    # if table_header_key is a year, and s does not contain a 4-digit year, then append the year to s
    if (
        table_header_key.isdigit()
        and len(table_header_key) == 4
        and not re.search(r"\b\d{4}\b", s)  # type: ignore
    ):
        s = f"{s} {table_header_key}"

    settings = {"DATE_ORDER": "DMY"}
    if "%" in s:
        return None, False
    # if s contains only the year return 1st of Jan of that year
    if isinstance(s, str) and re.fullmatch(r"\d{4}", s):
        year = int(s)
        return pd.Timestamp(year=year, month=1, day=1), True

    # if no fallback date other than the one from the url
    if prev_date is None and not _contains_year(s) and election_year:
        prev_date = pd.Timestamp(int(election_year.group(1)), 1, 1)

    if prev_date:
        # if s does not contain a 4-digit year and has Jan as substring and the month of prev_date is December, then set  prev_date to 01.01 of next year
        if (
            "jan" in s.lower() and prev_date.month == 12 and not _contains_year(s)  # type: ignore
        ):
            print("WARNING: Niche use case!! Check.")
            prev_date = pd.Timestamp(year=prev_date.year + 1, month=1, day=1)
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
            if parsed_date > cutoff_date:
                return None, prev_date_year_given
            return pd.Timestamp(parsed_date), prev_date_year_given
        else:
            return None, False

    except Exception:
        print(f"Error parsing date: {s} from {url}")
        return None, False


def _contains_year(s: str) -> bool:
    return bool(re.search(r"\b\d{4}\b", s))


class WikipediaPollingFetcher:
    def __init__(self, url: str, headers: Optional[List[str]] = None):
        self.url = url
        self.headers = headers

    def fetch_tables(self, country: str) -> Dict[str, List[pd.DataFrame]]:
        """Prefer the first table under the section whose heading equals the current year.
        If not found, try previous year. Fallback to all tables on the page.
        """
        try:
            headers = {
                "User-Agent": "FarRightTrackerBot/1.0 (https://github.com/brjezierski/far-right-tracker-europe; brjezierski@gmail.com)"
            }

            r = requests.get(self.url, timeout=30, headers=headers)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            years = []
            # get years between current year and current year -10
            for y in range(datetime.utcnow().year, datetime.utcnow().year - 10, -1):
                years.append(str(y))
            # Use headers from constructor if provided, otherwise fall back to years
            specific_headers = self.headers if self.headers else years
            excluded_headers = (
                COUNTRY_EXCLUDED_HEADERS[country]
                if country in COUNTRY_EXCLUDED_HEADERS
                else []
            )
            if type(specific_headers) is str:
                specific_headers = [specific_headers]
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
                    if text != specific_header or text in excluded_headers:
                        continue

                    # Find the first table following this header, before the next heading at the same or higher level
                    node = header
                    header_level = int(header.name[1])
                    stopping_headers = possible_headers[:header_level]

                    df_dict = {}
                    node_header_text = ""
                    table_counter = 0
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
                                # Create table name for debug output
                                table_name_for_debug = f"{country}_{specific_header}_{node_header_text if node_header_text else f'table_{table_counter}'}"
                                table_counter += 1

                                # Use custom parser to preserve hierarchy
                                df = parse_html_table_with_hierarchy(
                                    str(node), self.url, table_name_for_debug
                                )
                                dfs = [df] if not df.empty else []

                                # save node as html for debugging
                                if DEBUG:
                                    with open(
                                        f"debug_{country}_{specific_header}_{node_header_text}.html",
                                        "w",
                                        encoding="utf-8",
                                    ) as f:
                                        f.write(str(node))
                                if node_header_text in excluded_headers:
                                    continue

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
            # if debug save these tables as html
            if DEBUG:
                for i, table in enumerate(soup.find_all("table")):
                    with open(
                        f"debug_{country}_fallback_table_{i}.html",
                        "w",
                        encoding="utf-8",
                    ) as f:
                        f.write(str(table))
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
            print(f"Fetched {len(tables_dict)} tables")
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
            # print(f"Processing table: {tables_key}")  # --- DEBUG ---
            for df in tables_dict[tables_key]:
                # save df locally for debugging
                if DEBUG:
                    df.to_csv(f"debug_{country}_{tables_key}.csv", index=False)

                # Process hierarchical column structure
                cols_info = process_hierarchical_columns(df)

                # Check for date column
                date_col_info = find_date_column(cols_info)

                if not date_col_info:
                    if DEBUG:
                        print(f"No date column found in table for {country}")
                    continue

                # Extract party columns (those with links) and fix empty names
                header_parties = []
                for col_info in cols_info:
                    if col_info["column_link"]:
                        column_name = col_info["column_name"]
                        # If name is empty, extract it from the link
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
                party_name_mapping = all_parties_with_positions["party_name_mapping"]

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

                    date, prev_date_year_given = parse_date(
                        date_str, self.url, prev_date, prev_date_year_given, tables_key
                    )
                    if DEBUG:
                        print("Parsed date:", date_str, "->", date)
                    if date:
                        prev_date = date

                    if date is None:
                        continue

                    # Skip election results
                    if "election" in pollster.lower() or "result" in pollster.lower():
                        continue

                    for party_col_info in potential_party_cols:
                        try:
                            party_name_from_table = party_col_info["column_name"]
                            # Use canonical name if available from mapping

                            party_name = party_name_mapping.get(
                                party_name_from_table, party_name_from_table
                            )
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
                                polling_value_str, party_name_from_table, country
                            )

                            if (
                                pd.isna(polling_value)
                                or polling_value > 100
                                or polling_value <= 0
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
                        except Exception:
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


def get_polling_source(country: str) -> List[str]:
    """Return urls for the polling source. Currently uses Wikipedia pages mapping."""
    if country in WIKI_POLLING_PAGES:
        return WIKI_POLLING_PAGES[country].get("links", [])
    return []


def get_polling_headers(country: str) -> Optional[List[str]]:
    """Return the list of headers for the given country."""
    if country in WIKI_POLLING_PAGES:
        return WIKI_POLLING_PAGES[country].get("headers")
    return None


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


def _extract_party_name(soup: BeautifulSoup) -> Optional[str]:
    h1 = soup.find("h1")
    if h1:
        party_display_name = h1.get_text(" ", strip=True)
        # remove anything in parentheses
        party_display_name = (
            re.sub(r"\s*\(.*?\)\s*", "", party_display_name).strip().replace("–", "-")
        )
        return party_display_name
    return None


def _fetch_political_position(url: str) -> Optional[dict]:
    try:
        headers = {
            "User-Agent": "FarRightTrackerBot/1.0 (https://github.com/brjezierski/far-right-tracker-europe; brjezierski@gmail.com)",
        }
        r = requests.get(url, timeout=30, headers=headers)
        r.raise_for_status()
    except Exception:
        print(f"Error fetching URL: {url}")
        return None
    soup = BeautifulSoup(r.text, "lxml")
    # extract party_name from h1 header
    party_display_name = _extract_party_name(soup)

    # retrieve the following table <table class="infobox vcard">
    table = soup.select_one("table.infobox.vcard") or soup.select_one("table.infobox")
    if not table:
        if DEBUG:
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
            "party_display_name": party_display_name,
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
    country_cache: Dict[str, Dict[str, str]] = {}

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
            print(f"Error loading cache from {cache_path}: {e}")

    selected_parties: List[str] = []
    party_metadata: Dict[str, Dict] = {}
    party_name_mapping: Dict[str, str] = {}  # maps table name to article name

    for p in parties:
        party_name_from_table = str(p.get("name") or "").strip()
        link = (p.get("link") or "").strip()
        if not party_name_from_table:
            continue
        party_description = country_cache.get(party_name_from_table)

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
                    country_cache[party_name_from_table] = party_description

        # Determine the canonical party name (prefer article name if available)
        party_name_canonical = party_name_from_table
        if party_description and party_description.get("party_display_name"):
            party_display_name = party_description.get("party_display_name")
            if party_display_name and party_name_from_table not in party_name_mapping:
                party_name_canonical = party_display_name
                if DEBUG:
                    print(
                        f"Mapping party name: {party_name_from_table} -> {party_display_name}"
                    )
                party_name_mapping[party_name_from_table] = party_display_name

        # Store party metadata
        is_far_right = False
        if party_description:
            political_position = party_description.get("political_position")
            if political_position and isinstance(political_position, str):
                political_position = (
                    political_position.strip().lower().replace(" ", "-")
                )
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
            if DEBUG:
                print(f"Saved parties.csv for {country}")
        except Exception as e:
            print(f"Error saving cache to {cache_path}: {e}")
    elif country == "France":
        if DEBUG:
            print("Skipping parties.csv save for France (manually maintained)")

    return {
        "selected_parties": selected_parties,
        "party_metadata": party_metadata,
        "party_name_mapping": party_name_mapping,
    }
