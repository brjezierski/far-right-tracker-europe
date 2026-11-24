import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures" / "data"


def test_update_no_scraping_builds_country_json_and_slim_summary(tmp_path):
    data_dir = tmp_path / "data"
    shutil.copytree(FIXTURES, data_dir)

    subprocess.run(
        [sys.executable, "-m", "frt.cli", "update", "--no-scraping", "Luxembourg"],
        check=True,
        env={**os.environ, "FRT_DATA_DIR": str(data_dir)},
        capture_output=True,
    )

    country = json.loads((data_dir / "countries" / "LU.json").read_text())
    assert country["iso2"] == "LU"
    assert "seriesByParty" in country

    summary = json.loads((data_dir / "summary.json").read_text())
    assert set(summary["countries"]["LU"]) == {
        "country",
        "iso2",
        "parties",
        "activeParties",
        "latestSupport",
    }


def test_check_flags_stale_and_missing_polls(tmp_path, monkeypatch):
    from frt import check

    data_dir = tmp_path / "data"
    shutil.copytree(FIXTURES, data_dir)
    monkeypatch.setattr(check, "COUNTRIES_DIR", data_dir / "countries")
    monkeypatch.setattr(check, "WIKI_POLLING_PAGES", {"Luxembourg": {}, "Malta": {}})
    monkeypatch.setattr(check, "get_country_iso_code", {"Luxembourg": "LU", "Malta": "MT"}.get)

    assert check.check_sources(max_age_days=100000) == ["Malta: no polls"]
    assert "Luxembourg: last poll" in check.check_sources(max_age_days=0)[0]
