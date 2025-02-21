import pandas as pd

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

def compute_combined_hire_date(df: pd.DataFrame) -> pd.DataFrame:
    """Use the earlier of Hire Date and Original Hire Date (if available)."""
    df = df.copy()
    df["Hire Date"] = pd.to_datetime(df["Hire Date"], errors="coerce", dayfirst=True)
    if "Original Hire Date" in df.columns:
        df["Original Hire Date"] = pd.to_datetime(df["Original Hire Date"], errors="coerce", dayfirst=True)
        df["Combined hire date"] = df[["Hire Date", "Original Hire Date"]].min(axis=1)
    else:
        df["Combined hire date"] = df["Hire Date"]
    return df

def compute_most_recent_day_worked(df: pd.DataFrame) -> pd.DataFrame:
    """Prefer Last Day over Resignation Date for departure info."""
    df = df.copy()
    if "Last Day" in df.columns:
        df["Last Day"] = pd.to_datetime(df["Last Day"], errors="coerce", dayfirst=True)
    if "Resignation Date" in df.columns:
        df["Resignation Date"] = pd.to_datetime(df["Resignation Date"], errors="coerce", dayfirst=True)
    
    df["Most recent day worked"] = df.get("Last Day")
    if "Resignation Date" in df.columns:
        mask = df["Most recent day worked"].isna()
        df.loc[mask, "Most recent day worked"] = df.loc[mask, "Resignation Date"]
    return df

def clean_key_card_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the key card access data."""
    df = df.copy()
    
    # Extract employee_id from 'User' column with improved regex
    if 'User' in df.columns:
        # Extract numeric ID and convert to numeric type
        df['employee_id'] = df['User'].str.extract(r'^(\d+)').astype(float)
        
        print("\nSample of User column data with extracted IDs:")
        sample_df = pd.concat([
            df['User'],
            df['employee_id']
        ], axis=1).head(10)
        print(sample_df)
        
        # Print value counts to see extraction results
        print("\nEmployee ID extraction stats:")
        print(f"Total rows: {len(df)}")
        print(f"Rows with valid IDs: {df['employee_id'].notna().sum()}")
        print(f"Rows without IDs: {df['employee_id'].isna().sum()}")
    
    # Parse Date/time with explicit format
    df['parsed_time'] = pd.to_datetime(
        df['Date/time'],
        format="%d/%m/%Y %H:%M:%S",
        dayfirst=True,
        errors='coerce'
    )
    
    # Create date_only from parsed_time
    df['date_only'] = df['parsed_time'].dt.floor('d')
    
    # Add day of week
    df['day_of_week'] = df['date_only'].dt.strftime('%A')
    
    return df

def clean_employee_info(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and preprocess the employee information."""
    df = df.copy()
    
    # Convert Employee # to employee_id and ensure it's numeric
    df = df.rename(columns={'Employee #': 'employee_id'})
    df['employee_id'] = pd.to_numeric(df['employee_id'], errors='coerce')
    
    print("\nEmployee info stats:")
    print(f"Total employees: {len(df)}")
    print(f"Employees with valid IDs: {df['employee_id'].notna().sum()}")
    print(f"Unique employee IDs: {df['employee_id'].nunique()}")
    
    # Convert date columns
    date_columns = ['Hire Date', 'Original Hire Date', 'Resignation Date', 
                   'Employment Status: Date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            print(f"Converted {col} to datetime")
    
    # Add computed date columns
    df = compute_combined_hire_date(df)
    df = compute_most_recent_day_worked(df)
    
    # Clean up Working Status
    if 'Working Status' in df.columns:
        df['Working Status'] = df['Working Status'].str.strip()
        print("\nUnique Working Status values:")
        print(df['Working Status'].value_counts())
    
    return df

def merge_key_card_with_employee_info(
    key_card_df: pd.DataFrame,
    employee_df: pd.DataFrame
) -> pd.DataFrame:
    """Merge key card data with employee information."""
    print("\nBefore merge:")
    print("Key card shape:", key_card_df.shape)
    print("Employee shape:", employee_df.shape)
    
    # Both DataFrames should have 'employee_id' column at this point
    if 'employee_id' not in key_card_df.columns or 'employee_id' not in employee_df.columns:
        raise KeyError("Both DataFrames must have 'employee_id' column")
        
    # Ensure employee_id is numeric in both DataFrames
    key_card_df = key_card_df.copy()
    employee_df = employee_df.copy()
    
    # Print sample of IDs before merge
    print("\nFirst few employee IDs from key card data:")
    print(key_card_df[['User', 'employee_id']].head())
    print("\nFirst few employee IDs from employee info:")
    print(employee_df[['employee_id', 'Last name, First name']].head())
    
    # Merge the DataFrames
    merged_df = pd.merge(
        key_card_df,
        employee_df,
        on='employee_id',
        how='left'
    )
    
    # Debug: Check the merge results
    print("\nAfter merge:")
    print("Merged shape:", merged_df.shape)
    print("\nColumns with high null counts (>50%):")
    null_counts = merged_df.isnull().sum()
    high_nulls = null_counts[null_counts > len(merged_df) * 0.5]
    print(high_nulls)
    
    # Print sample of merged data
    print("\nSample of merged data:")
    sample_cols = ['employee_id', 'User', 'Last name, First name', 'Working Status', 'Location']
    print(merged_df[sample_cols].head(10))
    
    return merged_df

def add_time_analysis_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add additional time-based analysis columns to the DataFrame."""
    df = df.copy()
    
    # Ensure Date/time is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['Date/time']):
        df['Date/time'] = pd.to_datetime(df['Date/time'], dayfirst=True)
    
    # Add hour of day
    df['hour'] = df['Date/time'].dt.hour
    
    # Add time period categories
    df['time_period'] = pd.cut(
        df['hour'],
        bins=[-1, 9, 12, 14, 17, 24],
        labels=['Early Morning', 'Morning', 'Lunch', 'Afternoon', 'Evening']
    )
    
    return df
