"""Filesystem locations shared across the pipeline.

The data directory can be overridden with ``FRT_DATA_DIR`` (useful for tests).
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = Path(os.environ.get("FRT_DATA_DIR", REPO_ROOT / "data"))
COUNTRIES_DIR = DATA_DIR / "countries"
SOURCES_PATH = REPO_ROOT / "pipeline" / "config" / "polling_sources.json"
OVERRIDES_PATH = REPO_ROOT / "pipeline" / "config" / "classification_overrides.json"
