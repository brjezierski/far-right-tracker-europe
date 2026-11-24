"""User-Agent sent to Wikipedia, as its bot policy asks for a way to contact the operator.

The contact (an email or URL) is read from ``SCRAPER_CONTACT`` so it stays out of the repository.
"""

from __future__ import annotations

import os

REPO_URL = "https://github.com/brjezierski/far-right-tracker-europe"


def user_agent() -> str:
    contact = os.environ.get("SCRAPER_CONTACT", "").strip()
    details = f"{REPO_URL}; {contact}" if contact else REPO_URL
    return f"FarRightTrackerBot/1.0 ({details})"
