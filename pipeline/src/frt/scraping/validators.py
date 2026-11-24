"""Remember each source page's ETag / Last-Modified so unchanged pages can be skipped.

The validators live next to the country's CSVs (``page_validators.json``), so the nightly refresh
commits them with the data and the next run can send them back as conditional request headers.
"""

from __future__ import annotations

import json
from pathlib import Path

from frt.config import COUNTRIES_DIR


def _path(iso2: str) -> Path:
    return COUNTRIES_DIR / iso2 / "page_validators.json"


def load_validators(iso2: str) -> dict[str, dict[str, str]]:
    """Return {page url: {"etag": ..., "lastModified": ...}}, or {} if none were saved."""
    try:
        return json.loads(_path(iso2).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_validators(iso2: str, validators: dict[str, dict[str, str]]) -> None:
    path = _path(iso2)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(validators, ensure_ascii=False, indent=2), encoding="utf-8")


def conditional_headers(validators: dict[str, str] | None) -> dict[str, str]:
    """Request headers that ask the server to answer 304 if the page is unchanged."""
    headers = {}
    if validators and validators.get("etag"):
        headers["If-None-Match"] = validators["etag"]
    if validators and validators.get("lastModified"):
        headers["If-Modified-Since"] = validators["lastModified"]
    return headers


def response_validators(headers) -> dict[str, str]:
    found = {"etag": headers.get("ETag"), "lastModified": headers.get("Last-Modified")}
    return {k: v for k, v in found.items() if v}
