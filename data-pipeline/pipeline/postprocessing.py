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


def remove_anomalous_values(
    df: pd.DataFrame, threshold: float = 10.0, debug: bool = False
) -> pd.DataFrame:
    """
    Remove datapoints that are anomalously high or low compared to adjacent points.

    For each party, removes datapoints whose value differs by more than threshold
    from both the previous and next datapoints (either higher or lower).

    Args:
        df: DataFrame with columns ['date', 'party', 'polling_value', ...]
        threshold: Minimum absolute difference required to consider a point anomalous
        debug: Print debug information

    Returns:
        Filtered DataFrame with anomalous values removed
    """
    if df.empty:
        return df

    # Ensure date column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    rows_to_remove = []
    rows_removed_debug = []

    for party_name in df["party"].unique():
        party_df = df[df["party"] == party_name].copy()
        # Sort by date to ensure correct ordering
        party_df = party_df.sort_values("date").reset_index(drop=True)

        for i in range(len(party_df)):
            current_value = party_df.loc[i, "polling_value"]
            current_date = party_df.loc[i, "date"]
            original_idx = party_df.index[i]

            # Check if we have both previous and next points
            if i > 0 and i < len(party_df) - 1:
                prev_value = party_df.loc[i - 1, "polling_value"]
                next_value = party_df.loc[i + 1, "polling_value"]

                # Check if current value differs by more than threshold from both neighbors
                prev_diff = abs(prev_value - current_value)
                next_diff = abs(next_value - current_value)

                if prev_diff > threshold and next_diff > threshold:
                    # Find the original index in the input dataframe
                    mask = (
                        (df["party"] == party_name)
                        & (df["date"] == current_date)
                        & (df["polling_value"] == current_value)
                    )
                    original_indices = df[mask].index.tolist()
                    if original_indices:
                        rows_to_remove.extend(original_indices)
                        anomaly_type = (
                            "high"
                            if current_value > prev_value and current_value > next_value
                            else "low"
                        )
                        rows_removed_debug.append(
                            (
                                party_name,
                                current_date,
                                current_value,
                                prev_value,
                                next_value,
                                anomaly_type,
                            )
                        )

    if debug and rows_removed_debug:
        print(f"Removed {len(rows_removed_debug)} anomalous values:")
        for (
            party,
            date,
            curr_val,
            prev_val,
            next_val,
            anomaly_type,
        ) in rows_removed_debug[:10]:
            print(
                f"  {party} on {date.date()}: value={curr_val:.1f} ({anomaly_type}) "
                f"(prev={prev_val:.1f}, next={next_val:.1f})"
            )

    # Filter dataframe to remove anomalous points
    if rows_to_remove:
        return df.drop(index=rows_to_remove)
    else:
        return df


def filter_pre_2010_datapoints(
    df: pd.DataFrame, cutoff_year: int = 2010, debug: bool = False
) -> pd.DataFrame:
    """
    Remove all datapoints before a specified cutoff year.

    Args:
        df: DataFrame with columns ['date', 'party', 'polling_value', ...]
        cutoff_year: Year before which all datapoints will be removed (default: 2010)
        debug: Print debug information

    Returns:
        Filtered DataFrame with only data from cutoff_year onwards
    """
    if df.empty:
        return df

    # Ensure date column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    initial_count = len(df)
    cutoff_date = pd.Timestamp(year=cutoff_year, month=1, day=1)

    # Filter to keep only dates >= cutoff_date
    df_filtered = df[df["date"] >= cutoff_date].copy()

    if debug:
        removed_count = initial_count - len(df_filtered)
        print(f"Removed {removed_count} datapoints before {cutoff_year}")

    return df_filtered
