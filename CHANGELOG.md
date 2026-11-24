# Changelog

## Unreleased

- Lithuania: polls from 2025 and 2026 are read from their year sections instead of being dated 2028.
- Belgium: polls are current again. Added the 2029 polling page and restricted all three pages to the
  Flemish federal polls, so Wallonia, Brussels and seat-projection tables are no longer mixed into one series.
- Denmark: polling now reads "Opinion polling for the 2026 Danish general election" instead of the
  "next election" page, which became an article without poll tables after the election.
- The home page now explains the methodology below the map: where the polls come from, how parties are
  classified, and which countries are treated differently or left out.

- The scraper's contact address is no longer in the code. Set `SCRAPER_CONTACT` (a GitHub Actions
  secret for the daily refresh) to include it in the User-Agent sent to Wikipedia.

- Poland: Law and Justice is no longer shown while United Right is polled, so the stacked chart does
  not count the same voters twice. Overlaps are configured in `pipeline/config/party_overlaps.json`.
- Restructured the repository: the scraper is now the installable `frt` package under `pipeline/`.
- The built site is no longer committed; it is deployed with GitHub Pages Actions.
- Per-country JSON is generated at build time instead of committed; `summary.json` only carries the
  fields the map uses.
- Added CI (ruff, pytest, typecheck, lint), a Makefile, tests, and documentation.
- Removed the unused MapLibre map and its dependencies; countries without data are no longer clickable.
- Sped up the pipeline: a single-country run drops from about 8.5 minutes to under 30 seconds (vectorised outlier filtering and daily series, cached date parsing, a shared HTTP session with a rate limit instead of a fixed sleep, and parallel page and party fetches).
- Unchanged pages are skipped: each page's ETag / Last-Modified is stored in `data/countries/<ISO2>/page_validators.json` and sent back as a conditional request. A country whose pages all answer 304 keeps its CSVs. Use `frt update --force` to re-download everything, for example after changing the parsing code.
- `polling_data.csv` now records the pollster, and repeated polls are dropped on write.
