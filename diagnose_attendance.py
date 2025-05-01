#!/usr/bin/env python3
"""
Attendance Dashboard Diagnostic Tool

This script helps diagnose issues with the attendance dashboard by:
1. Loading and processing the latest data from key_card_access.csv
2. Running basic sanity checks on the data
3. Testing specific dashboard functionality
4. Logging detailed information about any errors encountered
"""

import pandas as pd
import logging
import traceback
import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Set up logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"diagnose_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("attendance_diagnose")

def check_type_consistency(df, column_name):
    """Check the type consistency of a specific column."""
    if column_name not in df.columns:
        logger.warning(f"Column '{column_name}' not found in DataFrame")
        return
    
    unique_types = df[column_name].apply(type).unique()
    logger.info(f"Column '{column_name}' contains types: {[t.__name__ for t in unique_types]}")
    
    # Check for mixed numeric types (str, int, float)
    numeric_types = [str, int, float]
    found_numeric_types = [t for t in unique_types if t in numeric_types]
    
    if len(found_numeric_types) > 1:
        logger.warning(f"Column '{column_name}' has mixed numeric types: {[t.__name__ for t in found_numeric_types]}")
        
        # Sample values of each type
        for type_cls in found_numeric_types:
            sample = df[df[column_name].apply(lambda x: isinstance(x, type_cls))][column_name].head(3)
            logger.info(f"Sample {type_cls.__name__} values: {sample.tolist()}")

def fix_employee_id_types(df):
    """Attempt to fix type inconsistencies in employee_id column."""
    if 'employee_id' not in df.columns:
        logger.warning("employee_id column not found")
        return df
    
    # Store original types for comparison
    original_types = df['employee_id'].apply(type).value_counts()
    logger.info(f"Original employee_id types: {original_types.to_dict()}")
    
    # Apply numeric conversion with error handling
    df['employee_id'] = pd.to_numeric(df['employee_id'], errors='coerce')
    
    # Use consistent float type for all values
    df['employee_id'] = df['employee_id'].astype('float64')
    
    # Check the result
    new_types = df['employee_id'].apply(type).value_counts()
    logger.info(f"After conversion employee_id types: {new_types.to_dict()}")
    
    return df

def test_daily_attendance_lookup(df, test_date):
    """Test the daily attendance lookup functionality."""
    try:
        from src.data_analysis.employee_metrics import get_daily_employee_attendance
        
        logger.info(f"Testing daily attendance lookup for {test_date}")
        
        # Ensure employee_id is numeric
        df = df.copy()
        df['employee_id'] = pd.to_numeric(df['employee_id'], errors='coerce').astype('float64')
        
        # Convert test_date to pandas Timestamp
        pd_test_date = pd.to_datetime(test_date)
        
        # Run the attendance lookup function
        daily_attendance = get_daily_employee_attendance(df, pd_test_date)
        
        logger.info(f"Daily attendance lookup successful, returned {len(daily_attendance)} rows")
        
        # Show a sample of the results
        if not daily_attendance.empty:
            logger.info(f"Sample result (first 3 rows):\n{daily_attendance.head(3)}")
            
            # Check for any type issues in the result
            check_type_consistency(daily_attendance, 'employee_id')
            
        return daily_attendance
        
    except Exception as e:
        logger.error(f"Error testing daily attendance lookup: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def main():
    """Main function to run diagnostics."""
    try:
        logger.info("Starting attendance dashboard diagnostics")
        
        # 1. Load the raw data
        logger.info("Loading key card data")
        key_card_path = Path("data/raw/key_card_access.csv")
        key_card_df = pd.read_csv(key_card_path)
        logger.info(f"Loaded {len(key_card_df)} rows from key card data")
        
        # 2. Check employee_id types in key card data
        check_type_consistency(key_card_df, 'User')
        
        # 3. Load employee info
        logger.info("Loading employee info")
        employee_path = Path("data/raw/employee_info.csv")
        employee_df = pd.read_csv(employee_path)
        logger.info(f"Loaded {len(employee_df)} rows from employee info")
        
        # 4. Check employee_id types
        check_type_consistency(employee_df, 'Employee #')
        
        # 5. Clean data using dashboard functions
        logger.info("Cleaning data using dashboard functions")
        from src.data_cleaning import clean_key_card_data, clean_employee_info, merge_key_card_with_employee_info
        
        key_card_df = clean_key_card_data(key_card_df)
        employee_df = clean_employee_info(employee_df)
        
        # 6. Check data types after cleaning
        check_type_consistency(key_card_df, 'employee_id')
        check_type_consistency(employee_df, 'employee_id')
        
        # 7. Merge the data
        logger.info("Merging datasets")
        combined_df = merge_key_card_with_employee_info(key_card_df, employee_df)
        logger.info(f"Combined data has {len(combined_df)} rows")
        
        # 8. Check employee_id types in merged data
        check_type_consistency(combined_df, 'employee_id')
        
        # 9. Fix employee_id types
        logger.info("Fixing employee_id types")
        combined_df = fix_employee_id_types(combined_df)
        
        # 10. Test the daily attendance lookup function
        yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        test_daily_attendance_lookup(combined_df, yesterday)
        
        logger.info("Diagnostics completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error in diagnostics: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()