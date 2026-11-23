"""Shared HTTP session and a polite rate limit for Wikipedia requests."""

from __future__ import annotations

import threading
import time

import requests

# Minimum gap between the start of two page downloads, across all threads.
MIN_REQUEST_INTERVAL = 1.0

session = requests.Session()
_throttle_lock = threading.Lock()
_next_slot = 0.0


def _wait_for_slot() -> None:
    global _next_slot
    with _throttle_lock:
        now = time.monotonic()
        start = max(now, _next_slot)
        _next_slot = start + MIN_REQUEST_INTERVAL
    if start > now:
        time.sleep(start - now)


def polite_get(
    url: str, headers: dict[str, str] | None = None, timeout: int = 30
) -> requests.Response:
    """GET through the shared keep-alive session, spaced by MIN_REQUEST_INTERVAL."""
    _wait_for_slot()
    return session.get(url, timeout=timeout, headers=headers)
