import pandas as pd
import numpy as np
import logging
import sys
import os

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.utils import validate_columns, handle_empty_dataframe

logger = logging.getLogger("attendance_dashboard.attendance_table")

def build_attendance_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build attendance table from key card data.
    
    This function creates a comprehensive attendance table by:
    1. Ensuring required time columns exist
    2. Creating a cross-product of all employees and dates
    3. Marking employee presence for each date
    4. Calculating attendance metrics
    
    Args:
        df: DataFrame with employee and attendance data
        
    Returns:
        DataFrame with attendance data for all employees across all dates
    """
    # Validate input
    if handle_empty_dataframe(df, "build_attendance_table", logger):
        return pd.DataFrame()
        
    required_columns = ["employee_id", "Last name, First name", "date_only"]
    if not validate_columns(df, required_columns, "build_attendance_table", logger):
        logger.error(f"Available columns: {df.columns.tolist()}")
        return pd.DataFrame()
    
    # Create a copy to avoid modifying the original
    logger.info(f"Building attendance table from {len(df):,} records")
    df = df.copy()
    
    # Ensure parsed_time exists for time calculations
    try:
        if 'parsed_time' not in df.columns:
            if 'Date/time' in df.columns:
                df['parsed_time'] = pd.to_datetime(df['Date/time'], dayfirst=True, errors='coerce')
                logger.info("Added missing parsed_time column from Date/time")
            elif 'Date_Parsed' in df.columns:
                df['parsed_time'] = pd.to_datetime(df['Date_Parsed'], errors='coerce')
                logger.info("Using Date_Parsed column for parsed_time")
            else:
                logger.warning("No time information found. Cannot calculate arrival times.")
                df['parsed_time'] = pd.NaT
    except Exception as e:
        logger.error(f"Error processing time columns: {str(e)}")
        df['parsed_time'] = pd.NaT
    
    # Log a sample for debugging (not showing employee details in logs)
    logger.debug(f"Sample data: {df[['employee_id', 'date_only', 'parsed_time']].head(3).to_dict('records')}")
    
    # Apply filters for employees
    try:
        location_mask = (df["Location"] == "London UK")
        working_mask = (df["Working Status"] == "Hybrid")
        
        # Handle missing is_full_time column
        if "is_full_time" in df.columns:
            full_time_mask = (df["is_full_time"] == True)
        else:
            logger.warning("is_full_time column not found. Assuming all employees are full-time.")
            full_time_mask = pd.Series(True, index=df.index)
            
        london_hybrid_ft_mask = location_mask & working_mask & full_time_mask
        
        # Log filtering results
        logger.info(f"Filtering: {len(df[london_hybrid_ft_mask]):,} of {len(df):,} rows match London+Hybrid+FT criteria")
    except Exception as e:
        logger.error(f"Error applying filters: {str(e)}")
        # Continue without filtering if there's an error
    
    # OPTIMIZATION: Vectorized approach for building employee-date combinations
    try:
        # Extract unique employees and dates
        unique_employees = df[["employee_id", "Last name, First name"]].drop_duplicates()
        unique_dates = pd.DataFrame({'date_only': df["date_only"].unique()})
        
        # Create cross-product using merge
        logger.info(f"Creating cross-product of {len(unique_employees):,} employees and {len(unique_dates):,} dates")
        employee_index = np.arange(len(unique_employees))
        date_index = np.arange(len(unique_dates))
        
        # Create meshgrid of indices
        employee_idx, date_idx = np.meshgrid(employee_index, date_index)
        
        # Create cross-product DataFrame
        cross_df = pd.DataFrame({
            'employee_idx': employee_idx.flatten(),
            'date_idx': date_idx.flatten()
        })
        
        # Map indices to actual values
        cross_df = cross_df.merge(
            unique_employees.reset_index(drop=True),
            left_on='employee_idx',
            right_index=True
        )
        
        cross_df = cross_df.merge(
            unique_dates.reset_index(drop=True),
            left_on='date_idx',
            right_index=True
        )
        
        # Drop temporary index columns
        cross_df = cross_df.drop(['employee_idx', 'date_idx'], axis=1)
        
        # Rename for compatibility
        cross_df = cross_df.rename(columns={"Last name, First name": "employee_name"})
        
        logger.info(f"Created cross-product with {len(cross_df):,} rows")
    except Exception as e:
        logger.error(f"Error creating employee-date cross product: {str(e)}")
        
        # Fallback to previous implementation if vectorized approach fails
        logger.warning("Falling back to iterative cross-product construction")
        employee_dates = []
        for _, employee in unique_employees.iterrows():
            for date in df["date_only"].unique():
                employee_dates.append({
                    "employee_id": employee["employee_id"],
                    "employee_name": employee["Last name, First name"],
                    "date_only": date
                })
        
        cross_df = pd.DataFrame(employee_dates)
    
    try:
        # Mark presence by checking if there's data for each employee-date
        attendance = (
            df.groupby(["employee_id", "date_only"])
            .size()
            .reset_index(name="visits")
        )
        
        # Merge attendance data with cross-product
        merged = cross_df.merge(
            attendance,
            on=["employee_id", "date_only"],
            how="left"
        )
        
        # Fill missing values and create required columns
        merged["visits"] = merged["visits"].fillna(0)
        merged["is_present"] = merged["visits"] > 0
        merged["present"] = merged["visits"].map({0: "No"}).fillna("Yes")  # For backward compatibility
        
        # Calculate days attended per employee
        days_attended = (
            merged[merged["is_present"] == True]
            .groupby("employee_id")
            .size()
            .reset_index(name="days_attended")
        )
        
        # Merge attendance metrics back to the main table
        final_df = merged.merge(days_attended, on="employee_id", how="left")
        
        # Sort by employee name and date
        final_df = final_df.sort_values(["employee_name", "date_only"])
        
        logger.info(f"Completed attendance table with {len(final_df):,} rows")
        logger.debug(f"Present days: {merged['is_present'].sum():,} of {len(merged):,} total employee-days")
        
        return final_df
    except Exception as e:
        logger.error(f"Error creating final attendance table: {str(e)}")
        return pd.DataFrame()