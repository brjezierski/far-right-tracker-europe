# Known issues

Carried over from the old README.

## Frontend

- Clicking a country without data (e.g. Bosnia) fails with `Page "/country/[iso2]/page" is missing param ... in generateStaticParams()`. Countries without data should not be clickable.

## Data

- Rows scraped before the `pollster` column was added have no pollster and still contain exact duplicates
  until that country is re-scraped. If a source table has no usable pollster cell, two real polls with the
  same date, party and value are collapsed as a repeat.
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
