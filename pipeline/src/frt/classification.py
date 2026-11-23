from __future__ import annotations

import json
import logging
from functools import lru_cache

import pandas as pd

from frt.config import OVERRIDES_PATH

logger = logging.getLogger(__name__)

CATEGORIES = ["far-right"]


@lru_cache(maxsize=1)
def load_overrides() -> dict:
    """Load the per-country allow and deny lists of party names."""
    if not OVERRIDES_PATH.exists():
        return {"allow": {}, "deny": {}}
    with open(OVERRIDES_PATH, encoding="utf-8") as f:
        return json.load(f)


def is_party_far_right(
    political_position: str,
    ideology: str,
    categories: list,
    iso2: str | None = None,
    party: str | None = None,
) -> bool:
    """Determine if a party is far-right.

    The category must appear in the political position. The ideology is only consulted when the
    position is empty. The per-country allow and deny lists take precedence over the text.
    """
    if iso2 and party:
        overrides = load_overrides()
        if party in overrides["deny"].get(iso2, []):
            return False
        if party in overrides["allow"].get(iso2, []):
            return True

    if pd.isna(political_position):
        political_position = ""
    if pd.isna(ideology):
        ideology = ""

    position_normalized = (political_position or "").strip().lower().replace(" ", "-")
    ideology_normalized = (ideology or "").strip().lower().replace(" ", "-")

    text = position_normalized or ideology_normalized
    return any(category in text for category in categories)
