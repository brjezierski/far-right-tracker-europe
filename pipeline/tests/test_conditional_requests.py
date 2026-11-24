import json
import shutil
from pathlib import Path

import frt.build as build_module
import frt.scraping.polling as polling
import frt.scraping.validators as validators
from frt.scraping.validators import conditional_headers, response_validators

FIXTURES = Path(__file__).parent / "fixtures" / "data"


class FakeResponse:
    def __init__(self, status_code=200, headers=None, text="<html></html>"):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def test_conditional_headers_and_response_validators():
    stored = response_validators(
        {"ETag": 'W/"abc"', "Last-Modified": "Mon, 01 Jan 2026 00:00:00 GMT"}
    )
    assert stored == {"etag": 'W/"abc"', "lastModified": "Mon, 01 Jan 2026 00:00:00 GMT"}
    assert conditional_headers(stored) == {
        "If-None-Match": 'W/"abc"',
        "If-Modified-Since": "Mon, 01 Jan 2026 00:00:00 GMT",
    }
    assert conditional_headers(None) == {}
    assert response_validators({}) == {}


def test_validators_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(validators, "COUNTRIES_DIR", tmp_path)
    assert validators.load_validators("LU") == {}
    validators.save_validators("LU", {"https://x": {"etag": "1"}})
    assert validators.load_validators("LU") == {"https://x": {"etag": "1"}}


def test_fetcher_sends_validators_and_stops_on_304(monkeypatch):
    sent = {}

    def fake_get(url, headers=None, timeout=30):
        sent.update(headers)
        return FakeResponse(304)

    monkeypatch.setattr(polling, "polite_get", fake_get)
    fetcher = polling.WikipediaPollingFetcher("https://en.wikipedia.org/wiki/X")
    fetcher.validators = {"etag": "abc"}

    assert fetcher.fetch_tables("Luxembourg") == {}
    assert sent["If-None-Match"] == "abc"
    assert fetcher.not_modified
    assert fetcher.new_validators == {"etag": "abc"}


def test_fetcher_records_validators_from_a_full_response(monkeypatch):
    monkeypatch.setattr(
        polling, "polite_get", lambda *a, **k: FakeResponse(200, {"ETag": "new"}, "<html></html>")
    )
    fetcher = polling.WikipediaPollingFetcher("https://en.wikipedia.org/wiki/X")
    fetcher.prefetch("Luxembourg")
    assert fetcher.new_validators == {"etag": "new"}
    assert not fetcher.not_modified


def _setup_build(tmp_path, monkeypatch, status):
    data_dir = tmp_path / "data"
    shutil.copytree(FIXTURES, data_dir)
    countries = data_dir / "countries"
    for module in (build_module, validators):
        monkeypatch.setattr(module, "COUNTRIES_DIR", countries)
    monkeypatch.setattr(build_module, "DATA_DIR", data_dir)
    url = "https://en.wikipedia.org/wiki/X"
    monkeypatch.setattr(build_module, "get_polling_source", lambda country: [url])
    monkeypatch.setattr(build_module, "get_polling_headers", lambda country: None)
    monkeypatch.setattr(build_module, "WIKI_POLLING_PAGES", ["Luxembourg"])
    validators.save_validators("LU", {url: {"etag": "abc"}})
    calls = []

    def fake_get(u, headers=None, timeout=30):
        calls.append(headers)
        return FakeResponse(status, {"ETag": "abc"})

    monkeypatch.setattr(polling, "polite_get", fake_get)
    return data_dir, calls


def test_build_skips_a_country_whose_pages_are_unchanged(tmp_path, monkeypatch):
    data_dir, calls = _setup_build(tmp_path, monkeypatch, 304)
    csv = data_dir / "countries" / "LU" / "polling_data.csv"
    before = csv.read_text()

    build_module.build("Luxembourg")

    assert calls[0]["If-None-Match"] == "abc"
    assert csv.read_text() == before
    country = json.loads((data_dir / "countries" / "LU.json").read_text())
    assert country["iso2"] == "LU"


def test_build_force_ignores_stored_validators(tmp_path, monkeypatch):
    _, calls = _setup_build(tmp_path, monkeypatch, 304)
    build_module.build("Luxembourg", force=True)
    assert "If-None-Match" not in calls[0]
