# Changelog

## Unreleased

- Restructured the repository: the scraper is now the installable `frt` package under `pipeline/`.
- The built site is no longer committed; it is deployed with GitHub Pages Actions.
- Per-country JSON is generated at build time instead of committed; `summary.json` only carries the
  fields the map uses.
- Added CI (ruff, pytest, typecheck, lint), a Makefile, tests, and documentation.
- Removed the unused MapLibre map and its dependencies; countries without data are no longer clickable.
- Sped up the pipeline: a single-country run drops from about 8.5 minutes to under 30 seconds (vectorised outlier filtering and daily series, cached date parsing, a shared HTTP session with a rate limit instead of a fixed sleep, and parallel page and party fetches).
