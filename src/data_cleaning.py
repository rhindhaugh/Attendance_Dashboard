import pandas as pd
import numpy as np
from datetime import datetime, timedelta

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
        
        # Handle special cases
        special_cases = {
            "Arorra, Aakash": 378, 
            "Payne, James": 735    
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
    
    return result

def merge_key_card_with_employee_info(
    key_card_df: pd.DataFrame,
    employee_df: pd.DataFrame
) -> pd.DataFrame:
    """Merge key card data with employee information using optimized approach."""
    print("\nBefore merge:")
    print("Key card shape:", key_card_df.shape)
    print("Employee shape:", employee_df.shape)
    
    # Both DataFrames should have 'employee_id' column at this point
    if 'employee_id' not in key_card_df.columns or 'employee_id' not in employee_df.columns:
        raise KeyError("Both DataFrames must have 'employee_id' column")
    
    # Optimize: Only keep employee_df rows that have matching employee_ids in key_card_df
    # This reduces the size of the right-side DataFrame in the merge
    unique_ids_in_keycard = key_card_df['employee_id'].unique()
    filtered_employee_df = employee_df[employee_df['employee_id'].isin(unique_ids_in_keycard)]
    
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
    
    return merged_df

def add_time_analysis_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add additional time-based analysis columns to the DataFrame."""
    result = pd.DataFrame()
    
    # Ensure Date/time is datetime and extract hour efficiently
    if pd.api.types.is_datetime64_any_dtype(df['Date/time']):
        result['hour'] = df['Date/time'].dt.hour
    else:
        parsed_time = pd.to_datetime(df['Date/time'], dayfirst=True)
        result['hour'] = parsed_time.dt.hour
    
    # Add time period categories using efficient categorization
    result['time_period'] = pd.cut(
        result['hour'],
        bins=[-1, 9, 12, 14, 17, 24],
        labels=['Early Morning', 'Morning', 'Lunch', 'Afternoon', 'Evening']
    )
    
    return result
