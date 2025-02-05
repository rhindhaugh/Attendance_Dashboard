import pandas as pd

def calculate_visit_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count the number of visits (rows in the key card data) per employee_id.
    """
    return (
        df.groupby("employee_id")
        .size()
        .reset_index(name="visit_count")
    )

def calculate_average_arrival_hour(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate average hour of arrival for each employee_id.
    Uses the 'timestamp' column (renamed from 'Date/time' during cleaning).
    """
    df = df.copy()
    # Use 'timestamp' instead of 'Date/time' to match cleaned data
    df["arrival_hour"] = df["timestamp"].dt.hour
    return (
        df.groupby("employee_id")["arrival_hour"]
        .mean()
        .reset_index(name="avg_arrival_hour")
    )
