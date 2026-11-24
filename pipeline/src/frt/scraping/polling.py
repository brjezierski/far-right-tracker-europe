from __future__ import annotations

import logging
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateparser import parse

from frt.scraping.parties import annotate_parties_positions, clean_text
from frt.scraping.sources import COUNTRY_EXCLUDED_HEADERS
from frt.scraping.tables import (
    extract_hierarchical_value_by_level,
    extract_value_from_hierarchical_tuple,
    find_date_column,
    parse_all_tables_from_soup,
    parse_html_table_with_hierarchy,
)
from frt.scraping.values import (
    extract_party_name_from_link,
    get_latest_polling_value,
    get_polling_value,
)
from frt.series import calculate_latest_total_support

logger = logging.getLogger(__name__)


def normalize_party_name(s: str) -> str:
    return re.sub(r"\W+", "", s.lower()).replace("_", "").replace("party", "")


def parse_date(
    s: str | tuple,
    url: str,
    prev_date: pd.Timestamp | None = None,
    prev_date_year_given: bool = False,
    table_header_key: str = "",
) -> tuple[pd.Timestamp | None, bool]:
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
        table_header_key.isdigit() and len(table_header_key) == 4 and not re.search(r"\b\d{4}\b", s)  # type: ignore
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
            logger.info("WARNING: Niche use case!! Check.")
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
        logger.info(f"Error parsing date: {s} from {url}")
        return None, False


def _contains_year(s: str) -> bool:
    return bool(re.search(r"\b\d{4}\b", s))


class WikipediaPollingFetcher:
    def __init__(self, url: str, headers: list[str] | None = None):
        self.url = url
        self.headers = headers

    def fetch_tables(self, country: str) -> dict[str, list[pd.DataFrame]]:
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
                COUNTRY_EXCLUDED_HEADERS[country] if country in COUNTRY_EXCLUDED_HEADERS else []
            )
            if type(specific_headers) is str:
                specific_headers = [specific_headers]
            possible_headers = ["h1", "h2", "h3", "h4", "h5"]

            for specific_header in specific_headers:
                # Look for heading elements with text equal to the year
                for header in soup.find_all(possible_headers):
                    span = header.find("span", class_="mw-headline")
                    text = span.get_text(strip=True) if span else header.get_text(strip=True)
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
                            [h.name for h in node.find_all(possible_headers)] if node else []
                        )
                        if len(headers_in_node) > 0:
                            for _header_in_node in headers_in_node:
                                # get span and text
                                span_in_node = node.find("span", class_="mw-headline")
                                text_in_node = (
                                    span_in_node.get_text(strip=True)
                                    if span_in_node
                                    else node.get_text(strip=True)
                                )
                                # remove [edit] from text
                                node_header_text = re.sub(r"\[edit\]$", "", text_in_node).strip()

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
                                if logger.isEnabledFor(logging.DEBUG):
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
                                logger.info(f"Error parsing table: {e}")
                                if len(df_dict) > 0:
                                    return df_dict
                                pass

                    if len(df_dict) > 0:
                        return df_dict
            # Fallback: return all tables
            logger.info("No specific year tables found, returning all tables.")
            # Parse all tables with custom parser
            fallback_dfs = parse_all_tables_from_soup(soup)
            # if debug save these tables as html
            if logger.isEnabledFor(logging.DEBUG):
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
                logger.info(f"Error fetching specific year tables, returning all tables. {e}")
                r = requests.get(self.url, timeout=30)
                soup = BeautifulSoup(r.text, "lxml")
                fallback_dfs = parse_all_tables_from_soup(soup)
                return {"": fallback_dfs}
            except Exception:
                return {}

    def fetch_latest_and_series(
        self, country: str, categories: list[str]
    ) -> tuple[float | None, dict[str, list[dict]], dict[str, dict]]:
        """
        Returns tuple of (latest_total_support, series_by_party, party_metadata)
        series_by_party: {party: [{date, value}]}
        party_metadata: {party: {political_position, ideology, url, is_far_right}}
        Also annotates party political positions into data/all_parties.json using header links when available.
        """
        try:
            tables_dict = self.fetch_tables(country)
            logger.info(f"Fetched {len(tables_dict)} tables")
        except Exception:
            return None, {}, {}
        if len(tables_dict) == 0:
            logger.info(f"No tables found for {country} at {self.url}")
            return None, {}, {}

        series: dict[str, list[dict]] = {}
        latest_values: dict[str, float] = {}
        party_metadata: dict[str, dict] = {}
        latest_total: float | None = None

        # Heuristic: find the first table that looks like a poll list (has a date column and multiple party columns)
        for tables_key in tables_dict:
            # print(f"Processing table: {tables_key}")  # --- DEBUG ---
            for df in tables_dict[tables_key]:
                # save df locally for debugging
                if logger.isEnabledFor(logging.DEBUG):
                    df.to_csv(f"debug_{country}_{tables_key}.csv", index=False)

                # Process hierarchical column structure
                cols_info = process_hierarchical_columns(df)

                # Check for date column
                date_col_info = find_date_column(cols_info)

                if not date_col_info:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.info(f"No date column found in table for {country}")
                    continue

                # Extract party columns (those with links) and fix empty names
                header_parties = []
                for col_info in cols_info:
                    if col_info["column_link"]:
                        column_name = col_info["column_name"]
                        # If name is empty, extract it from the link
                        column_name = extract_party_name_from_link(col_info["column_link"])
                        # Clean party name (remove references like [b])
                        column_name = clean_text(column_name)
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
                prev_date: pd.Timestamp | None = None
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
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.info("Parsed date:", date_str, "->", date)
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
                            is_hierarchical = party_col_info.get("is_hierarchical", False)
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

                            if pd.isna(polling_value) or polling_value > 100 or polling_value <= 0:
                                continue

                            if party_name not in series:
                                series[party_name] = []
                            series[party_name].append(
                                {
                                    "date": date.date().isoformat(),
                                    "value": polling_value,
                                    "pollster": pollster,
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


def process_hierarchical_columns(df: pd.DataFrame) -> list[dict]:
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
                parent_info if isinstance(parent_info, tuple) else (str(parent_info), None)
            )
            child_text, child_link = (
                child_info if isinstance(child_info, tuple) and child_info != () else ("", None)
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
