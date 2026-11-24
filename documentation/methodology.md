# Methodology

This page describes how the numbers on the map are produced and where they can mislead. The code that
implements it is in `pipeline/src/frt/`.

## Sources

- **Polls:** the country-specific "Opinion polling for the … election" pages on Wikipedia. The pages
  and the table headers to read are listed in `pipeline/config/polling_sources.json`.
  Switzerland also reads the French Wikipedia article on the 2027 election, because English Wikipedia has
  no polling page for that cycle.
- **Party position and ideology:** the infobox of each party's Wikipedia article.

The project does not run or verify polls. It is only as accurate as Wikipedia's tables.

## What counts as far-right

A party is classified as far-right if its Wikipedia **political position** or **ideology** text contains
any of the categories in `CATEGORIES` (`pipeline/src/frt/classification.py`):

- `far-right`
- `national-conservatism`

Matching is case-insensitive and treats spaces as hyphens, so "Far right" and "far-right" both match. It is
a substring match: "Centre-right to far-right" counts.

Consequences worth knowing:

- The label depends on how Wikipedia editors describe a party on the day of the scrape. Disputed cases
  flip with edits to the infobox.
- Classification is done when building the data, not stored in the CSVs, so changing `CATEGORIES` and
  re-running `frt update --no-scraping` reclassifies everything without scraping again.
- A party's `political_position` and `ideology` can be corrected by hand in the country's `parties.csv`;
  the scraper does not overwrite that file.

## Country support figure

For each country the headline number is the **average, over the three most recent poll dates, of the
summed support of all far-right parties** polled on those dates (`series.py`). Parties that contributed
are reported as the active parties.

## Cleaning

Applied per country when scraping (`storage.py`, `postprocessing.py`):

1. Polls before 2010 are dropped.
2. Repeated polls (from overlapping source pages) are dropped: rows with the same date, pollster, party
   and value, before isolated points are counted.
3. Isolated points are removed: a point with fewer than two other points from the same party within one
   year.
4. For Spain, Austria, Poland, the Czech Republic and Portugal, anomalous values are removed: points that
   differ by more than 10 percentage points from both the previous and the next poll.

The chart series is a rolling average over 5 polls, interpolated to one value per day between the first and
last point.

## Known limits

- Countries without a usable Wikipedia polling page are not shown.
- Election results are not included, only polls.
- See [known-issues.md](known-issues.md) for specific data problems.
