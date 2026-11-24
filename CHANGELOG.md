# Changelog

## Unreleased

- Lithuania: polls from 2025 and 2026 are read from their year sections instead of being dated 2028.

- Poland: Law and Justice is no longer shown while United Right is polled, so the stacked chart does
  not count the same voters twice. Overlaps are configured in `pipeline/config/party_overlaps.json`.
- Restructured the repository: the scraper is now the installable `frt` package under `pipeline/`.
- The built site is no longer committed; it is deployed with GitHub Pages Actions.
- Per-country JSON is generated at build time instead of committed; `summary.json` only carries the
  fields the map uses.
- Added CI (ruff, pytest, typecheck, lint), a Makefile, tests, and documentation.
- Removed the unused MapLibre map and its dependencies; countries without data are no longer clickable.
- `polling_data.csv` now records the pollster, and repeated polls are dropped on write.
