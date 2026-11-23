# Data format

Everything lives under `data/`.

```
data/
├── summary.json                  # one entry per country, used by the map
└── countries/
    ├── <ISO2>/                   # committed, source of truth
    │   ├── polling_data.csv
    │   ├── parties.csv
    │   └── metadata.json
    └── <ISO2>.json               # generated, not committed
```

## `countries/<ISO2>/polling_data.csv`

One row per party per poll.

| Column | Meaning |
| --- | --- |
| `date` | Poll date, `YYYY-MM-DD` |
| `party` | Party name as used in the polling table |
| `polling_value` | Support in percent |
| `political_position` | From the party's Wikipedia infobox |
| `ideology` | From the party's Wikipedia infobox |
| `wikipedia_url` | The party's Wikipedia article |
| `candidate` | France only: the candidate polled |

## `countries/<ISO2>/parties.csv`

Party metadata. Edit this file to correct a position or ideology; the scraper keeps manual edits.

Columns: `party`, `political_position`, `ideology`, `wikipedia_url`, `party_display_name`, and for
some countries `display_name` or `candidate`.

## `countries/<ISO2>/metadata.json`

```json
{
  "country": "Luxembourg",
  "iso2": "LU",
  "sources": ["https://en.wikipedia.org/wiki/2023_Luxembourg_general_election"],
  "updatedAt": "2026-11-22T03:36:17Z",
  "latestTotal": 12.3
}
```

## `countries/<ISO2>.json` (generated)

Built from the CSVs by `frt update --no-scraping` and read by the site at build time. It holds the party
series, the rolling-average daily series and which parties were active on each date. It is git-ignored; the
deploy workflow and `make summary` create it.

## `summary.json`

```json
{
  "updatedAt": "2026-11-22T03:36:17Z",
  "countries": {
    "DE": {
      "country": "Germany",
      "iso2": "DE",
      "parties": ["Alternative for Germany"],
      "activeParties": ["Alternative for Germany"],
      "latestSupport": 27.95
    }
  }
}
```

`updatedAt` is the time of the last scrape.
