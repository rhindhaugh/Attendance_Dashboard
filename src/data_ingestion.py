import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
import os
import sys

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.config import DEFAULT_ANALYSIS_DAYS
from src.utils import optimize_dataframe_memory, handle_empty_dataframe

# Set up logger
logger = logging.getLogger("attendance_dashboard.data_ingestion")

def load_key_card_data(filepath: str, start_date: str = None, end_date: str = None, 
                       last_n_days: int = None, optimize_memory: bool = False) -> pd.DataFrame:
    """
    Load key card CSV data with optional date filtering.
    
    Args:
        filepath: Path to CSV file
        start_date: Optional start date string in format 'YYYY-MM-DD'
        end_date: Optional end date string in format 'YYYY-MM-DD'
        last_n_days: If provided, load only the last N days of data
        optimize_memory: Whether to optimize memory usage (slower but uses less RAM)
        
    Returns:
        Filtered DataFrame with key card data
    """
    try:
        # Check if file exists
        if not Path(filepath).exists():
            logger.error(f"File not found: {filepath}")
            return pd.DataFrame()
            
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
        
        # Log the loading operation
        logger.info(f"Loading key card data from {filepath}")
        
        # Load the data with specified dtypes and low_memory=False to avoid mixed type warnings
        df = pd.read_csv(filepath, dtype=dtype_dict, low_memory=False)
        
        # Check if data was loaded successfully
        if handle_empty_dataframe(df, "load_key_card_data", logger):
            return pd.DataFrame()
            
        # Log the initial data size
        initial_size = len(df)
        logger.info(f"Loaded {initial_size:,} records from key card data")
        
        # Calculate date range for filtering
        if last_n_days:
            logger.info(f"Applying last {last_n_days} days filter")
            end_dt = datetime.now() if not end_date else pd.to_datetime(end_date)
            start_dt = end_dt - timedelta(days=last_n_days)
            start_date = start_dt.strftime("%Y-%m-%d")
            end_date = end_date or datetime.now().strftime("%Y-%m-%d")
            logger.info(f"Calculated date range: {start_date} to {end_date}")
        
        # Only filter if dates are specified
        if start_date or end_date:
            logger.info(f"Applying date filter: {start_date or 'beginning'} to {end_date or 'end'}")
            
            # Use try-except to catch parsing errors
            try:
                # If Date/time is already a datetime, skip conversion
                if not pd.api.types.is_datetime64_any_dtype(df['Date/time']):
                    df['Date/time'] = pd.to_datetime(df['Date/time'], dayfirst=True, errors='coerce')
                    
                    # Log rows with parsing errors
                    nan_dates = df['Date/time'].isna().sum()
                    if nan_dates > 0:
                        logger.warning(f"Found {nan_dates} rows with invalid dates (NaT)")
                        
                # Apply date filters
                if start_date:
                    start_dt = pd.to_datetime(start_date)
                    df = df[df['Date/time'] >= start_dt]
                
                if end_date:
                    end_dt = pd.to_datetime(end_date)
                    df = df[df['Date/time'] <= end_dt]
                    
                # Log the filtering results
                filtered_size = len(df)
                reduction_pct = (1 - filtered_size / initial_size) * 100 if initial_size > 0 else 0
                logger.info(f"After date filtering: {filtered_size:,} records ({reduction_pct:.1f}% reduction)")
                
            except Exception as e:
                logger.error(f"Error during date filtering: {str(e)}")
                # Continue with unfiltered data if there's an error
        
        # Add date_only column for faster date comparisons
        try:
            if 'Date/time' in df.columns:
                if not pd.api.types.is_datetime64_any_dtype(df['Date/time']):
                    df['Date/time'] = pd.to_datetime(df['Date/time'], dayfirst=True, errors='coerce')
                df['date_only'] = df['Date/time'].dt.date
                logger.debug("Added date_only column")
        except Exception as e:
            logger.error(f"Error adding date_only column: {str(e)}")
            
        # Optimize memory usage if requested
        if optimize_memory:
            logger.info("Optimizing memory usage for key card data")
            df = optimize_dataframe_memory(df, logger)
            
        return df
    
    except Exception as e:
        logger.error(f"Critical error loading key card data: {str(e)}")
        return pd.DataFrame()
    
    return df

def load_employee_info(filepath: str, optimize_memory: bool = False) -> pd.DataFrame:
    """
    Load employee information data.
    
    Args:
        filepath: Path to CSV file
        optimize_memory: Whether to optimize memory usage (slower but uses less RAM)
        
    Returns:
        DataFrame with employee information
    """
    try:
        # Check if file exists
        if not Path(filepath).exists():
            logger.error(f"File not found: {filepath}")
            return pd.DataFrame()
            
        logger.info(f"Loading employee information from {filepath}")
        
        # Define dtypes for columns to prevent mixed types
        dtype_dict = {
            'Employee ID': str,
            'Last name, First name': str,
            'Location': str,
            'Working Status': str,
            'Status': str,
            'Division': str
        }
        
        # Load the data with specified dtypes
        df = pd.read_csv(filepath, dtype=dtype_dict, low_memory=False)
        
        # Check if data was loaded successfully
        if handle_empty_dataframe(df, "load_employee_info", logger):
            return pd.DataFrame()
        
        logger.info(f"Loaded {len(df):,} employee records")
        
        # Auto-detect date columns and convert them
        date_columns = ['Hire Date', 'Original Hire Date', 
                       'Employment Status: Date', 'Resignation Date']
        
        for col in date_columns:
            if col in df.columns:
                try:
                    # First try with dayfirst=True since our data is likely in DD/MM/YYYY format
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
                    
                    # Log NaT values
                    nan_dates = df[col].isna().sum()
                    if nan_dates > 0:
                        logger.warning(f"Found {nan_dates} invalid dates in column '{col}'")
                except Exception as e:
                    logger.error(f"Error converting column '{col}' to datetime: {str(e)}")
        
        # Optimize memory usage if requested
        if optimize_memory:
            logger.info("Optimizing memory usage for employee data")
            df = optimize_dataframe_memory(df, logger)
            
        return df
        
    except Exception as e:
        logger.error(f"Critical error loading employee info: {str(e)}")
        return pd.DataFrame()

def load_employment_history(filepath: str, optimize_memory: bool = False) -> pd.DataFrame:
    """
    Load employment history data from CSV file.
    
    Args:
        filepath: Path to CSV file with employment history
        optimize_memory: Whether to optimize memory usage
        
    Returns:
        DataFrame with employment history data
    """
    try:
        # Check if file exists
        if not Path(filepath).exists():
            logger.error(f"Employment history file not found: {filepath}")
            return pd.DataFrame()
            
        logger.info(f"Loading employment history from {filepath}")
        
        # Define column types
        dtype_dict = {
            'Employee': str,
            'Employment Status': 'category',  # Use category for low cardinality columns
        }
        
        # Load the CSV file
        df = pd.read_csv(filepath, dtype=dtype_dict, low_memory=False)
        
        # Check if data was loaded successfully
        if handle_empty_dataframe(df, "load_employment_history", logger):
            return pd.DataFrame()
        
        # Ensure Date is parsed as datetime
        try:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            
            # Log NaT values
            nan_dates = df['Date'].isna().sum()
            if nan_dates > 0:
                logger.warning(f"Found {nan_dates} invalid dates in employment history")
                
        except Exception as e:
            logger.error(f"Error converting 'Date' to datetime: {str(e)}")
            # Continue with potentially invalid dates
        
        # Log summary information
        logger.info(f"Loaded employment history data: {len(df):,} rows")
        
        # Log unique employment statuses
        if 'Employment Status' in df.columns:
            statuses = df['Employment Status'].unique()
            logger.info(f"Employment statuses: {', '.join(str(s) for s in statuses)}")
        
        # Optimize memory if requested
        if optimize_memory:
            logger.info("Optimizing memory usage for employment history data")
            df = optimize_dataframe_memory(df, logger)
            
        return df
        
    except Exception as e:
        logger.error(f"Critical error loading employment history: {str(e)}")
        return pd.DataFrame()

def calculate_default_date_range(days=DEFAULT_ANALYSIS_DAYS):
    """
    Calculate default date range based on today's date.
    
    Args:
        days: Number of days to look back (default: from config)
    
    Returns:
        tuple: (start_date, end_date) as YYYY-MM-DD strings
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Format dates as YYYY-MM-DD strings
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    logger.debug(f"Calculated default date range: {start_str} to {end_str} ({days} days)")
    return start_str, end_str

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
