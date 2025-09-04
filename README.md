# Europe Nationalist Parties Support Map

This project displays a choropleth map of Europe showing support percentages for nationalist (often far-right populist) parties by country. Hover to see party names and current support; click a country to view time-series trends, toggle time ranges, and see data sources.

## Architecture

- Frontend: Next.js (React) + TypeScript + MapLibre GL JS + `@macrostrat/choropleth` utilities + ECharts for time-series.
- Data pipeline: Python module scraping Wikipedia for active nationalist parties, fetching polling/aggregated vote-intention per country, and outputting JSON to `data/` consumed by the frontend.
- Update: Daily via a GitHub Actions workflow.

## Data sources

- Party lists: Wikipedia - List of active nationalist parties in Europe
  https://en.wikipedia.org/wiki/List_of_active_nationalist_parties_in_Europe
- Polling data priority order per country (automatically selected per availability):
  1. Politico Europe Poll of Polls (preferred where available)
  2. Wikipedia country-specific opinion polling pages (scraped tables)
  3. National polling aggregators (manual mappings can be added in `data-pipeline/sources.yaml`)

Each country view shows the concrete sources used.

## Getting started

Prerequisites:

- Node.js 18+
- Python 3.10+

Install and run:

1. Install frontend deps
   ```bash
   cd frontend
   npm install
   ```
2. Install data pipeline deps and fetch data
   ```bash
   cd ../data-pipeline
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python -m pipeline.update
   ```
3. Start the frontend (in another terminal)
   ```bash
   cd ../frontend
   npm run dev
   ```
4. Open http://localhost:3000

## Updating data daily

- A GitHub Actions workflow `.github/workflows/update-data.yml` runs daily to refresh JSON under `data/` and commit changes.

## Project structure

- `frontend/` - Next.js app serving the map and country detail pages, reading JSON from `../data`.
- `data-pipeline/` - Python scraping/ETL code. Produces:
  - `../data/summary.json` - country-level latest support and parties
  - `../data/countries/<ISO2>.json` - time series and sources
- `data/` - Generated JSON files.

## Notes and limitations

- Party ideology labels are contentious; the pipeline relies on Wikipedia's categorization and may include parties with varying characteristics.
- Poll availability varies by country. Some countries may rely on election results where no polling exists; this will be indicated in sources. The gradient reflects the sum of support for parties categorized as nationalist in that country.

## Development tips

- To change the color scale or thresholds, edit `frontend/lib/colors.ts`.
- To add or pin a polling source for a country, update `data-pipeline/sources.yaml`.

# Issues

- Croatia doesn't show up
- France: NR missing
- Czechia: some party missing
- UK, Ireland, Switzerland

Leaflet

# TODOs
- fix the projection
- fix Armenia, Montenegro
- only get national polls (e.g. Ireland)
- do not collect politicians (e.g. Kosovo)
- fix the graphs
- collect past polling data
- set up git repo
- allow for ideology to be selected on the go
