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
    """Calculate the average arrival hour for each employee."""
    df = df.copy()
    
    # Get first scan of each day for each employee
    first_scans = (
        df.sort_values(['employee_id', 'date_only', 'parsed_time'])
        .groupby(['employee_id', 'date_only'])
        .first()
        .reset_index()
    )
    
    # Calculate arrival hour from parsed_time
    first_scans['arrival_hour'] = first_scans['parsed_time'].dt.hour
    
    # Calculate average arrival hour per employee
    return (
        first_scans
        .groupby('employee_id')['arrival_hour']
        .mean()
        .round(2)
        .reset_index()
    )

def calculate_mean_arrival_time(times_series: pd.Series) -> tuple[str, list]:
    """
    Calculate mean arrival time while excluding outliers.
    
    Args:
        times_series: Series of datetime.time objects
        
    Returns:
        tuple: (formatted_mean_time, list_of_excluded_times)
    """
    # Handle empty series or all-NaN series
    if times_series.empty or times_series.isna().all():
        return None, []
        
    # Drop NaN values
    times_series = times_series.dropna()
    if times_series.empty:
        return None, []
    
    # Convert times to minutes since midnight
    try:
        minutes = pd.Series([
            t.hour * 60 + t.minute 
            for t in times_series
        ], index=times_series.index)
    except AttributeError:
        # In case we have datetime objects instead of time objects
        print("Converting datetime objects to time...")
        minutes = pd.Series([
            t.hour * 60 + t.minute if hasattr(t, 'hour') and hasattr(t, 'minute') else None
            for t in times_series
        ], index=times_series.index)
        minutes = minutes.dropna()
        if minutes.empty:
            return None, []
    
    # Calculate median
    median_minutes = minutes.median()
    
    # Define outlier threshold (2 hours = 120 minutes)
    threshold = 120
    
    # Identify and exclude outliers
    is_outlier = abs(minutes - median_minutes) > threshold
    clean_minutes = minutes[~is_outlier]
    excluded_times = times_series[is_outlier]
    
    if clean_minutes.empty:
        return None, list(excluded_times)
    
    # Calculate mean of non-outlier times
    mean_minutes = round(clean_minutes.mean())
    mean_hours = mean_minutes // 60
    mean_mins = mean_minutes % 60
    
    return f"{int(mean_hours):02d}:{int(mean_mins):02d}", list(excluded_times)