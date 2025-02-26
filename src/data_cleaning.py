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

def compute_most_recent_day_worked(df: pd.DataFrame) -> pd.DataFrame:
    """Prefer Last Day over Resignation Date for departure info."""
    df = df.copy()
    
    # Initialize with Last Day if it exists
    if "Last Day" in df.columns:
        df["Most recent day worked"] = df["Last Day"]
    else:
        df["Most recent day worked"] = pd.NaT
    
    # Fill in with Resignation Date where Last Day is missing
    if "Resignation Date" in df.columns:
        mask = df["Most recent day worked"].isna()
        df.loc[mask, "Most recent day worked"] = df.loc[mask, "Resignation Date"]
    
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

def clean_employee_info(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the employee information."""
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
    result = compute_most_recent_day_worked(result)
    
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
