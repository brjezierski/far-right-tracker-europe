"""Postprocessing functions for polling data."""

import pandas as pd


def remove_isolated_datapoints(
    df: pd.DataFrame, min_neighbors: int = 2, debug: bool = False
) -> pd.DataFrame:
    """
    Remove isolated datapoints from polling data.

    For each party, removes datapoints that have fewer than min_neighbors other
    datapoints within ±1 year time range.

    Args:
        df: DataFrame with columns ['date', 'party', 'polling_value', ...]
        min_neighbors: Minimum number of neighboring datapoints required to keep a point
        debug: Print debug information

    Returns:
        Filtered DataFrame with isolated datapoints removed
    """
    if df.empty:
        return df

    # Ensure date column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    rows_to_keep = []
    rows_removed = []
    for party_name in df["party"].unique():
        party_df = df[df["party"] == party_name].copy()

        for idx, row in party_df.iterrows():
            current_date = row["date"]
            one_year_before = current_date - pd.DateOffset(years=1)
            one_year_after = current_date + pd.DateOffset(years=1)

            # Count other datapoints in the ±1 year range (excluding current point)
            nearby_points = party_df[
                (party_df["date"] >= one_year_before)
                & (party_df["date"] <= one_year_after)
                & (party_df.index != idx)
            ]

            # Keep the point if it has at least min_neighbors neighbors
            if len(nearby_points) >= min_neighbors:
                rows_to_keep.append(idx)
            else:
                rows_removed.append((party_name, current_date, len(nearby_points)))

    if debug and rows_removed:
        print(f"Removed {len(rows_removed)} isolated datapoints:")
        for party, date, neighbors in rows_removed[:10]:
            print(f"  {party} on {date.date()}: {neighbors} neighbors")

    # Filter dataframe to keep only non-isolated points
    if rows_to_keep:
        return df.loc[rows_to_keep]
    else:
        return pd.DataFrame(columns=df.columns)
