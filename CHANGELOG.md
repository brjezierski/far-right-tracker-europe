# Changelog

## Unreleased

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
- `polling_data.csv` now records the pollster, and repeated polls are dropped on write.
- Added `frt check`, run after the nightly refresh, which fails the job on broken or redirected source links and on stale or missing polls.
