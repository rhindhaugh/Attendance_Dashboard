import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
import sys

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.config import SPECIAL_EMPLOYEE_IDS
from src.utils import optimize_dataframe_memory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("attendance_dashboard.data_cleaning")

def load_key_card_data(filepath: str) -> pd.DataFrame:
    """Load key card data from CSV file."""
    df = pd.read_csv(filepath)
    print("\nLoaded key card data columns:", df.columns.tolist())
    return df

def load_employee_info(filepath: str) -> pd.DataFrame:
    """Load employee information from CSV file."""
    df = pd.read_csv(filepath)
    print("\nLoaded employee info columns:", df.columns.tolist())
    return df

def compute_combined_hire_date(result: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """Use the earlier of Hire Date and Original Hire Date (if available)."""
    if "Original Hire Date" in df.columns:
        hire_date = pd.to_datetime(df["Hire Date"], errors="coerce", dayfirst=True)
        original_hire_date = pd.to_datetime(df["Original Hire Date"], errors="coerce", dayfirst=True)
        result["Combined hire date"] = pd.concat([hire_date, original_hire_date], axis=1).min(axis=1)
    else:
        result["Combined hire date"] = pd.to_datetime(df["Hire Date"], errors="coerce", dayfirst=True)
    return result

def compute_most_recent_day_worked(df: pd.DataFrame, max_data_date=None) -> pd.DataFrame:
    """
    Set Most recent day worked based on employee status.
    - For active employees: use the most recent date from key card data (max_data_date)
    - For inactive employees: use Employment Status: Date (resignation/termination date)
    
    Args:
        df: DataFrame with employee info
        max_data_date: The latest date from key card data
        
    Returns:
        DataFrame with Most recent day worked column added
    """
    df = df.copy()
    
    # Initialize with NaT
    df["Most recent day worked"] = pd.NaT
    
    # Check if Employment Status column exists
    if "Employment Status" in df.columns:
        # For inactive employees, use Employment Status: Date
        inactive_mask = df["Employment Status"] == "Inactive"
        if "Employment Status: Date" in df.columns:
            df.loc[inactive_mask, "Most recent day worked"] = df.loc[inactive_mask, "Employment Status: Date"]
        
        # For active employees, use max_data_date (latest date in key card data)
        active_mask = df["Employment Status"] == "Active"
        if max_data_date is not None:
            df.loc[active_mask, "Most recent day worked"] = max_data_date
        
        # Log the changes for verification
        print(f"\nSet Most recent day worked for {active_mask.sum()} active employees to: {max_data_date}")
        print(f"Set Most recent day worked for {inactive_mask.sum()} inactive employees based on Employment Status: Date")
    else:
        # If Employment Status column doesn't exist, use alternative approach
        print("\nWarning: 'Employment Status' column not found in employee data")
        
        # Check for Status column as an alternative
        if "Status" in df.columns:
            inactive_mask = df["Status"] == "Inactive"
            active_mask = df["Status"] == "Active"
            
            # For inactive employees, use Resignation Date if available
            if "Resignation Date" in df.columns:
                df.loc[inactive_mask, "Most recent day worked"] = df.loc[inactive_mask, "Resignation Date"]
            
            # For active employees, use max_data_date
            if max_data_date is not None:
                df.loc[active_mask, "Most recent day worked"] = max_data_date
            
            print(f"Using 'Status' column instead: {active_mask.sum()} active, {inactive_mask.sum()} inactive")
        else:
            # If no status column exists, set all employees to active with max_data_date
            print("No status column found. Setting all employees as active.")
            if max_data_date is not None:
                df["Most recent day worked"] = max_data_date
    
    return df

def clean_key_card_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the key card access data."""
    # Create a copy only of the columns we need to modify
    # This is more memory-efficient than df.copy()
    result = pd.DataFrame()
    
    # Extract employee_id from 'User' column with improved regex
    if 'User' in df.columns:
        # Extract numeric ID and convert to numeric type - use vectorized operations
        result['employee_id'] = pd.to_numeric(df['User'].str.extract(r'^(\d+)', expand=False), 
                                           errors='coerce')
        
        # Handle special cases - use IDs from configuration
        special_cases = {
            "Arorra, Aakash": 378, 
            "Payne, James": 735,
            "Mueller, Benjamin": SPECIAL_EMPLOYEE_IDS.get('BENJAMIN_MUELLER', 867),
            "Hindhaugh, Robert": SPECIAL_EMPLOYEE_IDS.get('ROBERT_HINDHAUGH', 849)
        }

        # After the regular extraction, check for special cases
        for special_name, special_id in special_cases.items():
            special_mask = df['User'].str.contains(special_name, regex=False)
            result.loc[special_mask, 'employee_id'] = special_id
        
        print(f"\nEmployee ID extraction stats:")
        print(f"Total rows: {len(df)}")
        print(f"Rows with valid IDs: {result['employee_id'].notna().sum()}")
        print(f"Rows without IDs: {result['employee_id'].isna().sum()}")
    
    # Optimize datetime parsing - if already parsed, don't parse again
    if 'Date/time' in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df['Date/time']):
            result['parsed_time'] = df['Date/time']
        else:
            # Use efficient vectorized parsing
            result['parsed_time'] = pd.to_datetime(
                df['Date/time'],
                format="%d/%m/%Y %H:%M:%S",
                dayfirst=True,
                errors='coerce'
            )
    
    # Create date_only from parsed_time using efficient vectorized operations
    result['date_only'] = result['parsed_time'].dt.floor('d')
    
    # Add day of week
    result['day_of_week'] = result['parsed_time'].dt.strftime('%A')
    
    # Copy only needed columns from original DataFrame to save memory
    needed_columns = ['User', 'Where', 'Event', 'Details']
    for col in needed_columns:
        if col in df.columns:
            result[col] = df[col]
    
    return result

def clean_employee_info(df: pd.DataFrame, max_data_date=None) -> pd.DataFrame:
    """
    Clean and preprocess the employee information.
    
    Args:
        df: DataFrame with employee info
        max_data_date: The latest date from key card data
    """
    # Create a copy only of needed columns
    result = pd.DataFrame()
    
    # Convert Employee # to employee_id and ensure it's numeric
    result['employee_id'] = pd.to_numeric(df['Employee #'], errors='coerce')
    
    print("\nEmployee info stats:")
    print(f"Total employees: {len(df)}")
    print(f"Employees with valid IDs: {result['employee_id'].notna().sum()}")
    print(f"Unique employee IDs: {result['employee_id'].nunique()}")
    
    # Convert date columns efficiently
    date_columns = ['Hire Date', 'Original Hire Date', 'Resignation Date', 
                   'Employment Status: Date']
    for col in date_columns:
        if col in df.columns:
            result[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            print(f"Converted {col} to datetime")
    
    # Add computed date columns
    result = compute_combined_hire_date(result, df)
    result = compute_most_recent_day_worked(result, max_data_date)
    
    # Copy other needed columns
    needed_columns = ['Last name, First name', 'Working Status', 'Location', 
                     'Division', 'Department', 'Employment Status']
    for col in needed_columns:
        if col in df.columns:
            result[col] = df[col]
    
    # Clean up Working Status
    if 'Working Status' in result.columns:
        result['Working Status'] = result['Working Status'].str.strip()
        print("\nUnique Working Status values:")
        print(result['Working Status'].value_counts())
    
    # Ensure Robert Hindhaugh is in the dataset
    if 849 not in result['employee_id'].values:
        # Add Rob's information
        rob_data = {
            'employee_id': 849,
            'Last name, First name': 'Hindhaugh, Robert',
            'Working Status': 'Hybrid',
            'Location': 'London UK',
            'Combined hire date': pd.to_datetime('07/01/2025', dayfirst=True),
            'Most recent day worked': max_data_date if max_data_date else pd.NaT,
            'Employment Status': 'Active'
        }
        
        # Append to result DataFrame
        rob_df = pd.DataFrame([rob_data])
        result = pd.concat([result, rob_df], ignore_index=True)
        print("\nAdded missing data for Robert Hindhaugh (ID: 849)")
    
    return result

def merge_key_card_with_employee_info(
    key_card_df: pd.DataFrame,
    employee_df: pd.DataFrame,
    history_df: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Merge key card data with employee information and add full-time indicators.
    
    Args:
        key_card_df: DataFrame with key card data
        employee_df: DataFrame with employee info
        history_df: Optional DataFrame with employment history
        
    Returns:
        Merged DataFrame with optional full-time indicators
    """
    print("\nBefore merge:")
    print("Key card shape:", key_card_df.shape)
    print("Employee shape:", employee_df.shape)
    
    # Both DataFrames should have 'employee_id' column at this point
    if 'employee_id' not in key_card_df.columns or 'employee_id' not in employee_df.columns:
        raise KeyError("Both DataFrames must have 'employee_id' column")
    
    # Optimize: Only keep employee_df rows that have matching employee_ids in key_card_df
    # or are special cases (like Rob Hindhaugh - ID 849)
    unique_ids_in_keycard = key_card_df['employee_id'].unique()
    special_ids_to_keep = [849]  # Rob Hindhaugh's ID
    
    # Create filter mask
    filter_mask = employee_df['employee_id'].isin(unique_ids_in_keycard) | employee_df['employee_id'].isin(special_ids_to_keep)
    filtered_employee_df = employee_df[filter_mask]
    
    print(f"Filtered employee DataFrame from {len(employee_df)} to {len(filtered_employee_df)} rows")
    
    # Merge the DataFrames
    merged_df = pd.merge(
        key_card_df,
        filtered_employee_df,
        on='employee_id',
        how='left'
    )
    
    print("After merge:")
    print("Merged shape:", merged_df.shape)
    
    # If history data is provided, add full-time indicators
    if history_df is not None:
        # Create name to ID mapping
        employee_name_to_id = create_employee_name_to_id_mapping(employee_df)
        
        # Create status lookup
        status_lookup = create_employment_status_lookup(history_df, employee_name_to_id)
        
        # Add full-time indicators
        merged_df = add_full_time_indicators(merged_df, status_lookup)
    
    return merged_df

def create_employee_name_to_id_mapping(employee_df: pd.DataFrame) -> dict:
    """
    Create mapping from employee names to IDs.
    
    Args:
        employee_df: DataFrame with employee info
        
    Returns:
        Dictionary mapping employee names to IDs
    """
    mapping = {}
    for _, row in employee_df.iterrows():
        if pd.notna(row['Last name, First name']) and pd.notna(row['employee_id']):
            mapping[row['Last name, First name']] = row['employee_id']
    
    print(f"Created name-to-ID mapping for {len(mapping)} employees")
    return mapping

def create_employment_status_lookup(history_df: pd.DataFrame, employee_name_to_id: dict) -> dict:
    """
    Create a lookup that allows determining an employee's status on any date.
    
    Args:
        history_df: DataFrame with employment history
        employee_name_to_id: Dictionary mapping employee names to IDs
        
    Returns:
        Dictionary mapping employee_id to sorted list of (date, status) tuples
    """
    # Create the lookup dictionary
    status_lookup = {}
    
    # Process each status change
    for _, row in history_df.iterrows():
        emp_name = row['Employee']
        date = row['Date']
        status = row['Employment Status']
        
        # Skip if we can't map the name to an ID
        if emp_name not in employee_name_to_id:
            continue
            
        emp_id = employee_name_to_id[emp_name]
        
        # Store the status change
        if emp_id not in status_lookup:
            status_lookup[emp_id] = []
            
        status_lookup[emp_id].append((date, status))
    
    # Sort status changes by date for each employee
    for emp_id in status_lookup:
        status_lookup[emp_id].sort(key=lambda x: x[0])
    
    print(f"Created status lookup for {len(status_lookup)} employees")
    return status_lookup

def is_full_time_on_date(emp_id: float, date: pd.Timestamp, status_lookup: dict) -> bool:
    """
    Determine if an employee was Full-Time on a specific date.
    
    Args:
        emp_id: Employee ID
        date: Date to check
        status_lookup: Dictionary from create_employment_status_lookup
        
    Returns:
        True if employee was Full-Time on date, False otherwise
    """
    if emp_id not in status_lookup:
        return False
        
    # Find the most recent status change before or on this date
    most_recent_status = None
    for change_date, status in status_lookup[emp_id]:
        if change_date <= date:
            most_recent_status = status
        else:
            break
            
    return most_recent_status == 'Full-Time'

def add_full_time_indicators(df: pd.DataFrame, status_lookup: dict = None) -> pd.DataFrame:
    """
    Add a column indicating if each employee was Full-Time on each date.
    
    This function efficiently adds a full-time indicator to each row by:
    1. Using vectorized operations where possible
    2. Only applying row-by-row processing when necessary
    3. Handling special cases with appropriate logging
    
    Args:
        df: DataFrame with key card and employee data
        status_lookup: Dictionary from create_employment_status_lookup (optional)
        
    Returns:
        DataFrame with 'is_full_time' column added
    """
    import logging
    try:
        logger = logging.getLogger("attendance_dashboard.data_cleaning")
    except:
        # Fallback if logger isn't initialized
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("attendance_dashboard.data_cleaning")
    
    # Create a copy to avoid modifying the original
    result = df.copy()
    
    # Validate necessary columns
    if 'employee_id' not in result.columns:
        logger.error("Cannot add full-time indicators: missing employee_id column")
        return df
        
    if 'date_only' not in result.columns and status_lookup is not None:
        logger.error("Cannot add full-time indicators using status lookup: missing date_only column")
        return df
    
    # Add the is_full_time column, defaulting to False
    result['is_full_time'] = False
    
    # OPTIMIZATION: Apply special cases using vectorized operations
    # Always mark Rob Hindhaugh (ID: 849) as full-time
    special_case_ids = [849]  # Add other special cases as needed
    for special_id in special_case_ids:
        mask = result['employee_id'] == special_id
        if mask.any():
            result.loc[mask, 'is_full_time'] = True
            logger.info(f"Marked {mask.sum()} rows for employee ID {special_id} as full-time")
    
    # If no status lookup provided, we're done (only special cases apply)
    if status_lookup is None or not status_lookup:
        logger.info(f"Added is_full_time indicator using only special cases")
        logger.info(f"Employees marked as Full-Time: {result['is_full_time'].sum()} rows")
        return result
    
    # OPTIMIZATION: Process each employee separately to minimize row-by-row operations
    for emp_id in result['employee_id'].dropna().unique():
        # Skip special cases that have already been handled
        if emp_id in special_case_ids:
            continue
            
        # Skip if this employee isn't in the status lookup
        if emp_id not in status_lookup:
            continue
            
        # Get status history for this employee
        status_history = status_lookup[emp_id]
        if not status_history:
            continue
            
        # Get all rows for this employee
        emp_mask = result['employee_id'] == emp_id
        emp_rows = result[emp_mask]
        
        # For each date, find the applicable status
        for date, status_rows in emp_rows.groupby('date_only'):
            # Find the most recent status as of this date
            most_recent_status = None
            for change_date, status in status_history:
                if change_date <= date:
                    most_recent_status = status
                else:
                    break
                    
            # Mark as full-time if the status is 'Full-Time'
            if most_recent_status == 'Full-Time':
                date_mask = (result['employee_id'] == emp_id) & (result['date_only'] == date)
                result.loc[date_mask, 'is_full_time'] = True
    
    logger.info(f"Added is_full_time indicator to {len(result)} rows")
    logger.info(f"Employees marked as Full-Time: {result['is_full_time'].sum()} rows")
    
    return result

def add_time_analysis_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add additional time-based analysis columns to the DataFrame."""
    # Create copy of the input DataFrame rather than creating a new empty one
    result = df.copy()
    
    # Ensure parsed_time is available and extract hour efficiently
    if 'parsed_time' in df.columns:
        result['hour'] = df['parsed_time'].dt.hour
    else:
        # If parsed_time is not found, check for Date/time
        if 'Date/time' in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df['Date/time']):
                result['hour'] = df['Date/time'].dt.hour
            else:
                parsed_time = pd.to_datetime(df['Date/time'], dayfirst=True)
                result['hour'] = parsed_time.dt.hour
        else:
            raise KeyError("Neither 'parsed_time' nor 'Date/time' column found in DataFrame")
    
    # Add time period categories using efficient categorization
    result['time_period'] = pd.cut(
        result['hour'],
        bins=[-1, 9, 12, 14, 17, 24],
        labels=['Early Morning', 'Morning', 'Lunch', 'Afternoon', 'Evening']
    )
    
    return result
