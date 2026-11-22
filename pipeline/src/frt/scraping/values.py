from __future__ import annotations

import logging
import re
import urllib.parse
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


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


def get_polling_value(val: Any, party: str = "", country: str = "") -> float | None:
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


def get_latest_polling_value(pts: list[dict]) -> float:
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
