import pandas as pd
import re

def clean_key_card_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the key card DataFrame:
    1. Rename 'Date/time' to 'timestamp'
    2. Extract employee_id from 'User' column
    3. Convert timestamp to datetime
    4. Sort by timestamp
    """
    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # 1. Rename date/time column
    df.rename(columns={"Date/time": "timestamp"}, inplace=True)

    # 2. Extract employee_id from User column (numeric part at start)
    df["employee_id"] = df["User"].str.extract(r"^(\d+)\s", expand=False)
    
    # Drop rows where we couldn't extract an employee_id (e.g., cleaners)
    df = df.dropna(subset=["employee_id"])
    
    # Convert employee_id to integer
    df["employee_id"] = df["employee_id"].astype(int)

    # 3. Convert timestamp to datetime (your data is in dd/mm/yyyy format)
    df["timestamp"] = pd.to_datetime(df["timestamp"], dayfirst=True, errors="coerce")
    
    # 4. Sort by timestamp
    df = df.sort_values(by="timestamp")

    return df

def add_time_analysis_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add time analysis columns:
    - date_only: Extract date from timestamp
    - day_of_week: Monday, Tuesday, etc.
    - time_only: HH:MM:SS
    - earliest_scan_time: First scan per day per employee
    """
    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Extract date (as date object)
    df['date_only'] = df['timestamp'].dt.date
    
    # Get day of week
    df['day_of_week'] = df['timestamp'].dt.day_name()
    
    # Get time only
    df['time_only'] = df['timestamp'].dt.time
    
    # Calculate earliest scan time per day per employee
    df['earliest_scan_time'] = df.groupby(
        ['date_only', 'employee_id']
    )['time_only'].transform('min')

    return df

def clean_employee_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the employee info DataFrame:
    1. Rename 'Employee #' to 'employee_id'
    2. Convert employee_id to integer, dropping any rows with missing values
    3. Handle any missing values
    """
    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # 1. Rename the column (matching exact name from CSV)
    df.rename(columns={"Employee #": "employee_id"}, inplace=True)
    
    # 2. Drop rows where employee_id is missing
    df = df.dropna(subset=['employee_id'])
    
    # 3. Convert employee_id to int
    df["employee_id"] = df["employee_id"].astype(int)
    
    return df

def merge_key_card_with_employee_info(key_card_df: pd.DataFrame, employee_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge key card data with employee info on 'employee_id'.
    Return the combined DataFrame.
    """
    merged_df = key_card_df.merge(employee_df, on="employee_id", how="left")
    return merged_df

def save_processed_data(df: pd.DataFrame, filepath: str):
    """Save the processed DataFrame to a file."""
    df.to_parquet(filepath, index=False)
