# Europe Nationalist Parties Support Map

This project displays a choropleth map of Europe showing support percentages for nationalist (often far-right populist) parties by country. Hover to see party names and current support; click a country to view time-series trends, toggle time ranges, and see data sources.

## Architecture

- Frontend: Next.js (React) + TypeScript + MapLibre GL JS + `@macrostrat/choropleth` utilities + ECharts for time-series.
- Data pipeline: Python module scraping Wikipedia for active nationalist parties, fetching polling/aggregated vote-intention per country, and outputting JSON to `data/` consumed by the frontend.
- Update: Daily via a GitHub Actions workflow.

## Data sources

- Polling data is taken from Wikipedia country-specific opinion polling pages (scraped tables)
- The party position and ideology is taken from its Wikipedia article

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

### Data Pipeline

1. Collect and save polling data for **all parties** found in polling tables (not just far-right)
2. Store data in **CSV format** instead of JSON for better data analysis compatibility
3. Include political position and ideology metadata for each party
4. Generate summary.json from CSV files rather than in-memory data


## Project structure

- `frontend/` - Next.js app serving the map and country detail pages, reading JSON from `../data`.
- `data-pipeline/` - Python scraping/ETL code. Produces:
  - `../data/summary.json` - country-level latest support and parties
  - `../data/countries/<ISO2>.json` - time series and sources
- `data/` - Generated JSON files.

### Data structure


#### Country Directories (`data/countries/{ISO2}/`)

Each country now has its own directory containing:

##### `polling_data.csv`
Time series polling data for all parties:
```csv
date,party,polling_value,political_position,ideology,wikipedia_url
2025-09-01,Social_Democratic_Party,25.0,Centre-left,Social democracy,https://en.wikipedia.org/wiki/...
2025-09-01,Alternative_for_Germany,17.2,Far-right,Right-wing populism,https://en.wikipedia.org/wiki/...
2025-08-29,Social_Democratic_Party,24.5,Centre-left,Social democracy,https://en.wikipedia.org/wiki/...
```

##### `parties.csv`
Party metadata:
```csv
party,political_position,ideology,wikipedia_url
Social_Democratic_Party,Centre-left,Social democracy,https://en.wikipedia.org/wiki/...
Alternative_for_Germany,Far-right,Right-wing populism,https://en.wikipedia.org/wiki/...
```

##### `metadata.json`
Country and source information:
```json
{
  "country": "Germany",
  "iso2": "DE",
  "sources": [
    {"type": "wikipedia", "url": "https://en.wikipedia.org/wiki/..."}
  ],
  "updatedAt": "2025-09-04T..."
}
```

#### Summary File (`data/summary.json`)

Generated from CSV data:
```json
{
  "countries": {
    "DE": {
      "country": "Germany",
      "iso2": "DE",
      "parties": ["Social_Democratic_Party", "Alternative_for_Germany", ...],
      "farRightParties": ["Alternative_for_Germany"],
      "latestSupport": 85.5,
      "latestFarRightSupport": 17.2,
      "latestUpdate": "2025-09-04T..."
    }
  },
  "updatedAt": "2025-09-04T..."
}
```

## Usage

```bash
# Run for all countries (full scraping)
python -m pipeline.update

# Run for specific country  
python -m pipeline.update Germany

# Rebuild summary.json from existing CSV data without scraping
python -m pipeline.update --no-scraping

# Rebuild summary for specific country from CSV data
python -m pipeline.update Germany --no-scraping
```

## Dynamic Far-Right Classification

The system now determines far-right classification dynamically when generating `summary.json`:

1. **No Pre-stored Classification**: The CSV files no longer store `is_far_right` flags
2. **Dynamic Evaluation**: Far-right status is determined by checking if any of the defined categories (`far-right`, `right-wing-populism`, `nationalism`) appear in the party's political position or ideology
3. **Flexible Categories**: You can modify the `CATEGORIES` list and rebuild the summary without re-scraping data
4. **No-Scraping Mode**: Use `--no-scraping` to rebuild `summary.json` from existing CSV data with current category definitions



COUNTRY_TABLE_HEADERS = {
    "Albania": ["Nationwide"],
    "Armenia": ["Opinion polls"],
    "Czech Republic": ["Polls"],
    "Ireland": ["National polls"],
}