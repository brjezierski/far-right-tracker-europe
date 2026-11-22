from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

CATEGORIES = ["far-right", "national-conservatism"]


def is_party_far_right(political_position: str, ideology: str, categories: list) -> bool:
    """Determine if a party is far-right based on political position and ideology."""
    if not political_position and not ideology:
        return False

    if pd.isna(political_position):
        political_position = ""
    if pd.isna(ideology):
        ideology = ""

    # Normalize text for comparison
    position_normalized = (political_position or "").strip().lower().replace(" ", "-")
    ideology_normalized = (ideology or "").strip().lower().replace(" ", "-")

    # Check if any category matches
    for category in categories:
        if category in position_normalized or category in ideology_normalized:
            return True

    return False
