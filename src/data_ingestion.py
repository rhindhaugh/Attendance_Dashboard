import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

def load_key_card_data(filepath: str, start_date: str = None, end_date: str = None, 
                       last_n_days: int = None) -> pd.DataFrame:
    """
    Load key card CSV data with optional date filtering.
    
    Args:
        filepath: Path to CSV file
        start_date: Optional start date string in format 'YYYY-MM-DD'
        end_date: Optional end date string in format 'YYYY-MM-DD'
        last_n_days: If provided, load only the last N days of data
        
    Returns:
        Filtered DataFrame with key card data
    """
    # Define dtypes for columns to prevent mixed types
    dtype_dict = {
        'User': str,
        'Event': str,
        'Where': str,
        'Date/time': str,
        'Card Number': str,  # Column 5
        'Door': str,         # Column 6
        'Employee #': str
    }
    
    # Load the data with specified dtypes
    df = pd.read_csv(filepath, dtype=dtype_dict)
    
    # If date filtering is requested, calculate dates
    if last_n_days:
        end_dt = datetime.now() if not end_date else pd.to_datetime(end_date)
        start_dt = end_dt - timedelta(days=last_n_days)
        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    
    # Only filter if dates are specified
    if start_date or end_date:
        # Parse dates consistently using the same method as before
        df['Date/time'] = pd.to_datetime(df['Date/time'], dayfirst=True)
        
        # Apply date filters
        if start_date:
            df = df[df['Date/time'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['Date/time'] <= pd.to_datetime(end_date)]
    
    return df

def load_employee_info(filepath: str) -> pd.DataFrame:
    """
    Load employee info CSV data.
    The important column here is 'Employee #'.
    """
    return pd.read_csv(filepath)

def load_employment_history(filepath: str) -> pd.DataFrame:
    """
    Load employment history data from CSV file.
    
    Args:
        filepath: Path to CSV file with employment history
        
    Returns:
        DataFrame with employment history data
    """
    # Load the CSV file
    df = pd.read_csv(filepath)
    
    # Ensure Date is parsed as datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    print(f"Loaded employment history data: {len(df)} rows")
    print(f"Employment statuses: {df['Employment Status'].unique()}")
    
    return df

def calculate_default_date_range(days=365):
    """
    Calculate default date range (last year)
    
    Args:
        days: Number of days to look back (default: 365)
    
    Returns:
        tuple: (start_date, end_date) as YYYY-MM-DD strings
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

if __name__ == "__main__":
    # Example usage
    base_path = Path(__file__).resolve().parent.parent  # go up one level from 'src'
    key_card_path = base_path / "data" / "raw" / "key_card_access.csv"
    employee_info_path = base_path / "data" / "raw" / "employee_info.csv"

    # Example 1: Load all data
    print("\nLoading all data:")
    key_card_df = load_key_card_data(str(key_card_path))
    print("Full key card shape:", key_card_df.shape)

    # Example 2: Load last 30 days
    print("\nLoading last 30 days:")
    recent_df = load_key_card_data(str(key_card_path), last_n_days=30)
    print("Recent key card shape:", recent_df.shape)

    # Example 3: Load specific date range
    print("\nLoading specific date range:")
    start_date, end_date = calculate_default_date_range(days=90)
    date_range_df = load_key_card_data(
        str(key_card_path),
        start_date=start_date,
        end_date=end_date
    )
    print("Date range key card shape:", date_range_df.shape)

    # Load employee info
    employee_info_df = load_employee_info(str(employee_info_path))
    print("\nEmployee info shape:", employee_info_df.shape)
