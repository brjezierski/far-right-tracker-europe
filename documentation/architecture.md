# Architecture

```
            ┌──────────────────────────── pipeline/src/frt ────────────────────────────┐
 Wikipedia ─►  scraping/ (sources, tables, polling, parties, values)                     │
            │      │                                                                     │
            │      ▼                                                                     │
            │  postprocessing ─► storage (CSV write, JSON build) ─► summary              │
            └──────────────────────────────────┬─────────────────────────────────────────┘
                                               ▼
                                            data/
                                               ▼
                           frontend/ (Next.js static export, reads ../data at build time)
                                               ▼
                                         GitHub Pages
```

## Pipeline modules

| Module | Responsibility |
| --- | --- |
| `cli.py` | `frt update` entry point |
| `build.py` | Orchestrates a run: scrape each country, save CSVs, rebuild JSON and summary |
| `config.py` | Paths; `FRT_DATA_DIR` overrides the data directory |
| `scraping/sources.py` | Reads `config/polling_sources.json` |
| `scraping/polling.py` | `WikipediaPollingFetcher`: fetches pages and turns tables into series |
| `scraping/tables.py` | Parses Wikipedia tables with merged and multi-level headers |
| `scraping/parties.py` | Looks up each party's position and ideology |
| `scraping/values.py` | Parses percentages and party names |
| `classification.py` | `CATEGORIES` and `is_party_far_right` |
| `postprocessing.py` | Filtering, rolling average and daily interpolation |
| `series.py` | Latest support and daily support series |
| `storage.py` | Writes `polling_data.csv`; reads CSVs back into the per-country JSON |
| `summary.py` | Builds `summary.json` |

## Workflows

- **CI** (`ci.yml`): ruff and pytest for the pipeline; typecheck and lint for the frontend. Runs on PRs.
- **Update data** (`update-data.yml`): daily at 23:00 UTC. Scrapes, commits `data/` only if it changed,
  then calls the deploy workflow.
- **Deploy site** (`deploy.yml`): generates the per-country JSON from the CSVs, builds the site and
  publishes it with GitHub Pages. The built site is not committed.

Pages must be configured with **Source: GitHub Actions** (Settings → Pages).
