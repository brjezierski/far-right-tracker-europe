# Known issues

Carried over from the old README.

## Frontend

- Clicking a country without data (e.g. Bosnia) fails with `Page "/country/[iso2]/page" is missing param ... in generateStaticParams()`. Countries without data should not be clickable.

## Data

- `polling_data.csv` contains exact duplicate rows (about 11,700 across 36 countries, 4,100 in DE), most likely
  from overlapping election pages. They are not removed because the CSV has no pollster column, so two real
  polls with the same date, party and value cannot be told apart from a duplicate. Dropping them changes
  published figures (e.g. GB latest support 24.87 -> 25.0). Fix by recording the pollster, then dedupe.
- Lithuania: latest 2024 polling does not match the source.
- Turkey: data needs checking.
- Lithuanian Green party is excluded even though it is in ECR.
- Reformed Political Party: classification unclear.

## Removed sources

Russia was dropped as a source; the config entry was:

```json
"Russia": {
  "links": [
    "https://en.wikipedia.org/wiki/Opinion_polling_for_the_2016_Russian_legislative_election",
    "https://en.wikipedia.org/wiki/Opinion_polling_for_the_2021_Russian_legislative_election",
    "https://en.wikipedia.org/wiki/Opinion_polling_for_the_2026_Russian_legislative_election"
  ],
  "headers": ["Pre-campaign polls"]
}
```
