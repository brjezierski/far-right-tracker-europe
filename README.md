# Far-Right Tracker Europe

[![CI](https://github.com/brjezierski/far-right-tracker-europe/actions/workflows/ci.yml/badge.svg)](https://github.com/brjezierski/far-right-tracker-europe/actions/workflows/ci.yml)
[![Code: MIT](https://img.shields.io/badge/code-MIT-blue.svg)](LICENSE)
[![Data: CC BY-SA 4.0](https://img.shields.io/badge/data-CC%20BY--SA%204.0-lightgrey.svg)](DATA_LICENSE.md)

An interactive map of polling support for far-right and national-conservative parties across Europe.
Drag the timeline to see how support has moved since 2010, hover a country for its parties, and click
it for the per-party time series and sources.

**Live site:** https://brjezierski.github.io/far-right-tracker-europe/

All numbers come from the opinion-polling pages on Wikipedia. What counts as "far-right" is explained in
[documentation/methodology.md](documentation/methodology.md), including the limits of that definition.

## How it works

```
Wikipedia polling pages ──► pipeline/ (Python, `frt`) ──► data/ (CSV + JSON) ──► frontend/ (Next.js) ──► GitHub Pages
```

- `pipeline/` scrapes the polling tables, annotates each party with its position and ideology, cleans the
  series and writes one folder of CSVs per country.
- `frontend/` is a static Next.js site (Leaflet map, ECharts charts) that reads `data/` at build time.
- Two GitHub Actions run it: **Refresh data** (daily, scrapes and commits changed data, then deploys) and
  **Build and deploy site** (on pushes that touch the frontend or data).

More detail: [architecture](documentation/architecture.md) · [data format](documentation/data-format.md) ·
[known issues](documentation/known-issues.md).

## Getting started

You need [uv](https://docs.astral.sh/uv/) and Node.js 20+ (`frontend/.nvmrc`).

```bash
make setup        # install Python and Node dependencies
make summary      # build the JSON the site needs from the CSVs already in data/ (no scraping)
make dev          # start the site at http://localhost:3000
```

Other commands:

```bash
make data                    # scrape every country (slow, hits Wikipedia)
make data COUNTRY=Germany    # scrape one country
make build                   # static site in frontend/out
make lint test               # what CI runs
```

The CLI behind these is `frt update [COUNTRY] [--no-scraping] [--force] [--debug]`; run it from `pipeline/` with
`uv run frt update --help`.

## Repository layout

| Path | What |
| --- | --- |
| `pipeline/` | Python package `frt`: scraping, classification, postprocessing, CLI, tests |
| `pipeline/config/polling_sources.json` | Which Wikipedia pages and table headers to scrape per country |
| `frontend/` | Next.js app |
| `data/` | Committed CSVs and metadata per country, plus `summary.json` |
| `documentation/` | Methodology, data format, architecture, known issues |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Wrong party, wrong number or a classification you disagree with?
Open a [data error issue](../../issues/new?template=data-error.md).

## Licence

Code is MIT ([LICENSE](LICENSE)). The data is derived from Wikipedia and is licensed CC BY-SA 4.0
([DATA_LICENSE.md](DATA_LICENSE.md)).
