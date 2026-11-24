# Contributing

## Setup

```bash
make setup
make summary
make dev
```

Needs [uv](https://docs.astral.sh/uv/) and Node.js 20+. Optionally install the hooks with
`pre-commit install`.

## Before opening a PR

```bash
make lint test
```

CI runs the same checks.

## Data changes

- The daily workflow commits scraped data. Do not hand-edit `polling_data.csv`; it is overwritten on the
  next scrape.
- To fix a party's position or ideology, edit that country's `parties.csv`. The scraper keeps manual
  edits.
- Wikipedia asks bots to identify an operator. Set `SCRAPER_CONTACT` (an email or URL) in your shell
  before scraping; the daily workflow reads it from the `SCRAPER_CONTACT` repository secret.
- To change which pages are scraped for a country, edit `pipeline/config/polling_sources.json`.
- To change what counts as far-right, see [methodology](documentation/methodology.md).

## Commits

Short, imperative subject lines. Keep code changes and data changes in separate commits.
