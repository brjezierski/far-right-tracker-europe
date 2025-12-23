import re
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import pycountry
import urllib.parse
from bs4 import BeautifulSoup


# Function to get ISO alpha-2 code for a country name
def get_country_iso_code(country_name):
    try:
        # Search for the country by name
        country = pycountry.countries.get(name=country_name)
        if country:
            return country.alpha_2  # Return the ISO alpha-2 code
        else:
            additional_countries = {
                "Turkey": "TR",
                "Russia": "RU",
                "Czech Republic": "CZ",
                "Kosovo": "XK",
                "Moldova": "MD",
                "Macedonia": "MK",
            }
            return additional_countries.get(
                country_name, f"Country '{country_name}' not found."
            )
    except Exception as e:
        return f"An error occurred: {e}"


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
PIPELINE_DIR = ROOT / "data-pipeline/pipeline"
COUNTRIES_DIR = DATA_DIR / "countries"

COUNTRIES_DIR.mkdir(parents=True, exist_ok=True)


def extract_party_name_from_link(link: str) -> str:
    """
    Extract party name from a Wikipedia link.

    Examples:
        /wiki/People%27s_Party_(Spain) -> People's Party
        /wiki/National_Rally -> National Rally

    Args:
        link: Wikipedia link (e.g., '/wiki/People%27s_Party_(Spain)')

    Returns:
        Extracted party name with URL decoding and formatting
    """
    if not link:
        return ""

    # Extract the part after /wiki/
    if "/wiki/" in link:
        link = link.split("/wiki/", 1)[1]

    # URL decode to handle %27 -> ' etc.
    link = urllib.parse.unquote(link)

    # Remove content in parentheses (e.g., "(Spain)")
    link = re.sub(r"\([^)]*\)", "", link)

    # Replace underscores with spaces
    link = link.replace("_", " ")

    # Clean up extra spaces
    link = re.sub(r"\s+", " ", link).strip()

    return link


def save_json(path: Path, data: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def extract_percentage_value(val: str) -> float:
    """
    Extract and normalize a percentage value from a string.
    Removes '%', replaces ',' with '.', strips whitespace, and takes the first number.

    Args:
        val: String containing a percentage value (e.g., "23.5%", "23,5", "23.5 (note)")

    Returns:
        A float representing the extracted percentage value.
    """
    return float(val.replace("%", "").replace(",", ".").strip().split()[0])


def get_polling_value(val: Any, party: str = "", country: str = "") -> Optional[float]:
    """
    Extract a polling value from various data types (string, number, pandas Series).

    Args:
        val: The value to extract from (can be str, int, float, pandas Series, or "-")
        party: Party name for error messages (optional)
        country: Country name for error messages (optional)

    Returns:
        Float value representing the polling percentage, or None if invalid/missing
        Returns None for "-" (dash) values

    Raises:
        ValueError: If the value cannot be converted to a valid polling number
    """
    if val is None:
        return None

    if isinstance(val, pd.Series):
        for i in range(len(val)):
            cell_val = get_polling_value(val.iloc[i], party, country)
            if cell_val is not None:
                return cell_val

    else:
        # Handle tuples
        if isinstance(val, tuple):
            for item in val:
                if item is not None:
                    val = item

        # Handle string values
        if isinstance(val, str):
            # Handle dash/missing values
            if val == "-":
                return None

            return extract_percentage_value(val)

        # Handle numeric values
        if isinstance(val, (int, float)):
            return float(val)

    # Handle pandas Series (coalition parties)
    if val is not None and hasattr(val, "iloc"):
        try:
            v = 0.0
            for i in range(len(val)):
                if isinstance(val.iloc[i], str):
                    val_i = extract_percentage_value(val.iloc[i])
                    # Sometimes multiple parties in a coalition have the support listed together multiple times
                    # Here we apply a heuristic: if the same polling value appears we assume the case is as above,
                    # otherwise the support is listed for coalition parties separately
                    if val_i != v:
                        v += val_i
            return v
        except Exception as e:
            error_msg = "Error converting Series to value"
            if party:
                error_msg += f" for {party}"
            if country:
                error_msg += f" in {country}"
            error_msg += f": {e}"
            raise ValueError(error_msg) from e

    # Unknown type
    raise ValueError(f"Unknown value type: {type(val)}")


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def get_latest_polling_value(pts: List[Dict]) -> float:
    """
    Get the average of the last 5 polling values from a list of polling data points.
    If fewer than 5 values exist, averages all available values.

    Args:
        pts: List of dicts with 'date' and 'value' keys

    Returns:
        The average value of the most recent (up to 5) polling data points
    """
    # Sort by date to ensure we get the most recent values
    sorted_pts = sorted(pts, key=lambda x: x["date"], reverse=True)
    # Take up to 5 most recent values
    recent_values = [pt["value"] for pt in sorted_pts[:5]]
    # Return the average
    return sum(recent_values) / len(recent_values)


def _convert_subsequent_header_rows_to_td(table):
    """
    Convert <th> elements to <td> in all header rows except the first.

    When a table has multiple rows of headers, only the first row should use <th> elements.
    Subsequent header rows should use <td> elements for proper hierarchical parsing.

    Args:
        table: BeautifulSoup table element (modified in-place)
    """
    rows = table.find_all("tr")
    if not rows:
        return

    # Find the first row that contains <th> elements
    first_header_row_idx = None
    for idx, row in enumerate(rows):
        if row.find("th"):
            first_header_row_idx = idx
            break

    if first_header_row_idx is None:
        return

    # Convert <th> to <td> in all subsequent rows that contain headers
    for idx in range(first_header_row_idx + 1, len(rows)):
        row = rows[idx]
        th_cells = row.find_all("th")

        # Check if this row looks like a header row (has mostly <th> and no substantial data)
        # or if it has any <th> elements at all in positions other than the first column
        if th_cells:
            for th_cell in th_cells:
                # Convert <th> to <td>
                th_cell.name = "td"


def _insert_rowspan_placeholders(table):
    """
    Insert placeholder <td> elements for positions covered by rowspan.

    When a cell has rowspan > 1, subsequent rows don't include <td> elements
    for that position. This function inserts empty placeholder <td> elements
    to maintain consistent column positions across all rows.

    Args:
        table: BeautifulSoup table element (modified in-place)
    """
    rows = table.find_all("tr")
    if not rows:
        return

    # Track active rowspans: dict mapping (row_idx, col_idx) -> remaining_span
    active_rowspans = {}

    for row_idx, row in enumerate(rows):
        cells = row.find_all(["td", "th"])

        # Build a map of column positions for this row
        col_idx = 0
        cells_to_insert = []  # List of (position, cell) tuples

        for cell in cells:
            # Skip columns that are covered by active rowspans
            while (row_idx, col_idx) in active_rowspans:
                # Insert placeholder td at this position
                placeholder = BeautifulSoup("", "lxml").new_tag("td")
                placeholder.string = ""
                cells_to_insert.append((col_idx, placeholder))
                col_idx += 1

            # Process current cell
            colspan = int(cell.get("colspan", 1))
            rowspan = int(cell.get("rowspan", 1))

            # Register this cell's rowspan for future rows
            if rowspan > 1:
                for span_row in range(row_idx + 1, row_idx + rowspan):
                    for span_col in range(col_idx, col_idx + colspan):
                        active_rowspans[(span_row, span_col)] = True

            col_idx += colspan

        # Check for any remaining rowspan positions after the last cell
        while (row_idx, col_idx) in active_rowspans:
            placeholder = BeautifulSoup("", "lxml").new_tag("td")
            placeholder.string = ""
            cells_to_insert.append((col_idx, placeholder))
            col_idx += 1

        # Insert placeholder cells in order
        if cells_to_insert:
            cells_list = list(row.find_all(["td", "th"]))
            for target_col_idx, placeholder in reversed(cells_to_insert):
                # Find the cell before which to insert
                current_col = 0
                insert_before = None
                for cell in cells_list:
                    if current_col == target_col_idx:
                        insert_before = cell
                        break
                    current_col += int(cell.get("colspan", 1))

                if insert_before:
                    insert_before.insert_before(placeholder)
                else:
                    # Append to end if no cell found
                    row.append(placeholder)


def parse_html_table_with_hierarchy(table_html: str) -> pd.DataFrame:
    """
    Parse an HTML table preserving hierarchical header structure with colspan/rowspan.

    For headers with subheaders (colspan > 1), creates columns like:
    ((parent_text, parent_link), (child_text, child_link))

    For values under hierarchical headers:
    - If value spans multiple columns: ((value, link), ())
    - If value is in specific subcolumn: ((), (value, link))

    Args:
        table_html: HTML string of the table

    Returns:
        DataFrame with hierarchical tuple column names and structured values
    """
    soup = BeautifulSoup(table_html, "lxml")
    table = soup.find("table")
    if not table:
        return pd.DataFrame()

    # Preprocess: Convert subsequent header rows from <th> to <td>
    _convert_subsequent_header_rows_to_td(table)

    # Preprocess table to insert placeholder td elements for rowspan-covered cells
    _insert_rowspan_placeholders(table)

    # Extract all rows
    rows = table.find_all("tr")
    if not rows:
        return pd.DataFrame()

    # Parse header rows to build column structure
    # Look for rows with <th> elements
    # We need ALL header rows for grid building (including styling rows)
    # but we'll identify which are substantive for column naming
    all_header_rows = []
    substantive_header_indices = []
    data_rows = []
    in_header_section = True

    for row in rows:
        th_elements = row.find_all("th")
        td_elements = row.find_all("td")

        if th_elements:
            # Row has th elements - could be header or data row
            # Check if it's a data row (has substantive td elements with data)
            if td_elements and len(td_elements) > len(th_elements):
                # More td than th - likely a data row
                data_rows.append(row)
                in_header_section = False
            else:
                # Likely a header row
                if in_header_section:
                    all_header_rows.append(row)
                    # Check if substantive (has text or links)
                    has_any_text = any(
                        cell.get_text(strip=True) for cell in th_elements
                    )
                    has_any_links = any(cell.find("a") for cell in th_elements)
                    if has_any_text or has_any_links:
                        substantive_header_indices.append(len(all_header_rows) - 1)
                else:
                    # Special row in data section
                    data_rows.append(row)
        elif td_elements:
            # Only td elements - definitely data row
            data_rows.append(row)
            in_header_section = False

    if not all_header_rows or not substantive_header_indices:
        return pd.DataFrame()

    # Build column structure from ALL header rows (preserves rowspan structure)
    # but use the last substantive row for column naming
    columns = _build_hierarchical_columns(all_header_rows, substantive_header_indices)

    # Parse data rows
    data = []
    for row in data_rows:
        cells = row.find_all(["th", "td"])

        if not cells:
            # Empty row, skip it
            continue

        # Check if this is a continuation row by examining the actual HTML cells
        # Continuation rows typically have:
        # 1. Fewer cells than columns (because early cells are spanned from above)
        # 2. No rowspan attribute on any of the cells
        # A proper data row will have rowspan on early cells (date/pollster) or have all cells

        # Count cells and check for rowspan in any cell
        has_any_rowspan = any(
            cell.get("rowspan") and int(cell.get("rowspan", 1)) > 1 for cell in cells
        )

        # If row has significantly fewer cells than columns AND no rowspan, it's a continuation
        # Typical first data rows have rowspan=3 on first few cells and ~17-20 cells total
        # Continuation rows have no rowspan and ~16-17 cells
        # Use a threshold: if < 90% of columns and no rowspan, skip
        # if (
        #     len(cells) < len(columns) * 0.90 and not has_any_rowspan
        # ):  # TODO uncomment for France
        #     # Likely a continuation row, skip it
        #     continue

        row_data = _parse_row_with_hierarchy(cells, columns)
        data.append(row_data)

    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    return df


def _extract_cell_info(cell) -> Tuple[str, Optional[str], int, int]:
    """
    Extract text, link, colspan, and rowspan from a table cell.

    Returns:
        (text, link, colspan, rowspan)
    """
    # Get text - preserve internal whitespace but strip leading/trailing
    text = cell.get_text(separator=" ", strip=True)

    # Get link if present
    link = None
    a_tag = cell.find("a")
    if a_tag and a_tag.get("href"):
        link = a_tag.get("href")

    # Get colspan and rowspan
    colspan = int(cell.get("colspan", 1))
    rowspan = int(cell.get("rowspan", 1))

    return text, link, colspan, rowspan


def _build_hierarchical_columns(
    header_rows: List, substantive_indices: List[int]
) -> List[Tuple]:
    """
    Build hierarchical column structure from header rows.

    Handles colspan/rowspan to create columns like:
    ((parent_text, parent_link), (child_text, child_link))
    or
    ((text, link), ()) for non-hierarchical columns

    Args:
        header_rows: All header rows (including styling rows)
        substantive_indices: Indices of rows with actual content (not styling)
    """
    if not header_rows or not substantive_indices:
        return []

    # Use the last substantive row for determining final columns
    last_substantive_idx = substantive_indices[-1]

    # Build a 2D grid to properly handle colspan and rowspan
    # grid[row][col] = (text, link, is_original, parent_col_start, parent_colspan)
    grid = []
    max_cols = 0

    for row_idx, row in enumerate(header_rows):
        cells = row.find_all(["th", "td"])
        grid.append([None] * 200)  # Start with large array

        col_idx = 0  # Current column position in grid

        for cell in cells:
            # Find next unfilled position
            while col_idx < len(grid[row_idx]) and grid[row_idx][col_idx] is not None:
                col_idx += 1

            text, link, colspan, rowspan = _extract_cell_info(cell)

            # Fill the grid for this cell and its span
            for r in range(row_idx, min(row_idx + rowspan, len(header_rows))):
                # Extend grid if needed
                while r >= len(grid):
                    grid.append([None] * 200)

                for c in range(col_idx, col_idx + colspan):
                    if r == row_idx and c == col_idx:
                        # Original cell position
                        grid[r][c] = (text, link, True, col_idx, colspan, rowspan)
                    else:
                        # Filled by span
                        grid[r][c] = (text, link, False, col_idx, colspan, rowspan)

                    max_cols = max(max_cols, c + 1)

            col_idx += colspan

    # Trim grid to actual size
    for i in range(len(grid)):
        grid[i] = grid[i][:max_cols]

    # Now determine final column structure from the last substantive header row
    if not grid or last_substantive_idx >= len(grid):
        return []

    last_row = grid[last_substantive_idx]
    columns = []

    for col_idx in range(len(last_row)):
        if last_row[col_idx] is None:
            columns.append((("", None), ()))
            continue

        text, link, is_orig, parent_col_start, colspan, rowspan = last_row[col_idx]

        # Look for parent header in earlier rows
        parent_text, parent_link = None, None

        for row_idx in range(len(grid) - 1):  # Check all rows except last
            if col_idx < len(grid[row_idx]) and grid[row_idx][col_idx] is not None:
                p_text, p_link, p_is_orig, p_col_start, p_colspan, p_rowspan = grid[
                    row_idx
                ][col_idx]

                # This is a parent if:
                # 1. It has colspan > 1 (covers multiple children)
                # 2. It doesn't span all the way to the last row
                # 3. It's different from current cell
                if (
                    p_colspan > 1
                    and (row_idx + p_rowspan) <= len(grid) - 1
                    and p_text != text
                ):
                    parent_text, parent_link = p_text, p_link
                    break

        if parent_text is not None:
            # Has a parent - hierarchical column
            columns.append(((parent_text, parent_link), (text, link)))
        else:
            # No parent - flat column
            columns.append(((text, link), ()))

    return columns


def _parse_row_with_hierarchy(cells: List, columns: List[Tuple]) -> List[Tuple]:
    """
    Parse a data row, handling colspan for hierarchical columns.

    For hierarchical columns with colspan values:
    - If value spans multiple subcolumns: ((value, link), ())
    - If value is in specific subcolumn: ((), (value, link))
    """
    row_data = []
    cell_idx = 0
    col_idx = 0

    while col_idx < len(columns):
        if cell_idx >= len(cells):
            # No more cells, fill with None
            row_data.append(((), ()))
            col_idx += 1
            continue

        cell = cells[cell_idx]
        text, link, colspan, rowspan = _extract_cell_info(cell)

        # Check if this column is hierarchical
        col = columns[col_idx]
        parent_info, child_info = col
        is_hierarchical = child_info != ()

        if colspan > 1:
            # Value spans multiple columns - put in parent position
            value_tuple = ((text, link), ())
            row_data.append(value_tuple)

            # Fill remaining spanned columns with empty parent values
            for _ in range(1, colspan):
                col_idx += 1
                if col_idx < len(columns):
                    row_data.append(((), ()))
        else:
            # Single column value
            if is_hierarchical:
                # Put in child position for hierarchical columns
                value_tuple = ((), (text, link))
            else:
                # Put in parent position for non-hierarchical columns
                value_tuple = ((text, link), ())
            row_data.append(value_tuple)

        cell_idx += 1
        col_idx += 1

    return row_data


def extract_value_from_hierarchical_tuple(value_raw, default=""):
    """
    Extract text value from hierarchical tuple structure used in parsed tables.

    Handles tuple structure: ((parent_val, parent_link), (child_val, child_link))
    - If parent has value: ((value, link), ()) - returns parent value
    - If child has value: ((), (value, link)) - returns child value
    - If not a tuple: returns the value as string

    Args:
        value_raw: The raw value from DataFrame cell (can be tuple or any type)
        default: Default value to return if no value found (default: "")

    Returns:
        Extracted string value or default
    """
    if isinstance(value_raw, tuple) and len(value_raw) == 2:
        parent_val, child_val = value_raw
        if isinstance(parent_val, tuple) and parent_val != () and parent_val[0]:
            # Value in parent position
            return str(parent_val[0])
        elif isinstance(child_val, tuple) and child_val != () and child_val[0]:
            # Value in child position
            return str(child_val[0])
        else:
            # Empty tuple structure
            return default
    elif value_raw is not None:
        return str(value_raw)
    else:
        return default


def parse_all_tables_from_soup(soup) -> List[pd.DataFrame]:
    """
    Parse all HTML tables from a BeautifulSoup object using the hierarchical parser.

    Args:
        soup: BeautifulSoup object containing HTML with tables

    Returns:
        List of DataFrames, one for each successfully parsed non-empty table
    """
    all_tables = soup.find_all("table")
    parsed_dfs = []
    for tbl in all_tables:
        try:
            df = parse_html_table_with_hierarchy(str(tbl))
            if not df.empty:
                parsed_dfs.append(df)
        except Exception:
            pass
    return parsed_dfs


def find_date_column(cols_info: List[Dict]) -> Optional[Dict]:
    """
    Find the date column from a list of column info dictionaries.

    Looks for columns with 'date', 'fieldwork', or 'conducted' in the column_name field.

    Args:
        cols_info: List of column info dicts with 'column_name' field

    Returns:
        The column info dict for the date column, or None if not found
    """
    for col_info in cols_info:
        column_name_lower = col_info.get("column_name", "").lower()
        if (
            "date" in column_name_lower
            or "fieldwork" in column_name_lower
            or "conducted" in column_name_lower
            or "tarih" in column_name_lower
            or "period" in column_name_lower
        ):
            return col_info
    return None


def extract_hierarchical_value_by_level(
    value_raw, is_parent: bool = False, is_hierarchical: bool = True, default: str = ""
) -> str:
    """
    Extract value from hierarchical tuple structure based on parent/child level.

    Handles tuple structure: ((parent_val, parent_link), (child_val, child_link))
    - If is_parent=True: extracts value from parent position
    - If is_parent=False and is_hierarchical=True: extracts child value only (no fallback)
    - If is_parent=False and is_hierarchical=False: extracts from parent position (non-hierarchical column)

    Args:
        value_raw: The raw value from DataFrame cell (can be tuple or any type)
        is_parent: Whether to extract parent-level value (default: False)
        is_hierarchical: Whether this is a hierarchical column (default: True)
        default: Default value to return if no value found (default: "")

    Returns:
        Extracted string value or default
    """
    if isinstance(value_raw, tuple) and len(value_raw) == 2:
        parent_val, child_val = value_raw
        if is_parent:
            # Looking for parent party value
            if isinstance(parent_val, tuple) and parent_val != () and parent_val[0]:
                return str(parent_val[0])
        else:
            # Looking for child party value
            if isinstance(child_val, tuple) and child_val != () and child_val[0]:
                return str(child_val[0])
            # For non-hierarchical columns, fall back to parent position
            elif (
                not is_hierarchical
                and isinstance(parent_val, tuple)
                and parent_val != ()
                and parent_val[0]
            ):
                return str(parent_val[0])
            # For hierarchical child parties, no fallback to parent
        return default
    elif value_raw is not None:
        return str(value_raw)
    else:
        return default
