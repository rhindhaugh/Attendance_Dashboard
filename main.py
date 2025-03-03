from src.data_ingestion import load_key_card_data, load_employee_info, calculate_default_date_range
from src.data_cleaning import (
    clean_key_card_data,
    clean_employee_info,
    merge_key_card_with_employee_info,
    add_time_analysis_columns
)
from src.data_analysis import (
    build_attendance_table,
    calculate_visit_counts,
    calculate_average_arrival_hour
)
from src.utils import setup_logging, safe_data_frame_operation
import argparse
from datetime import datetime, timedelta
import time
import gc
import logging

def main():
    """
    This function will:
    1. Load the key card data from data/raw/key_card_access.csv with date filtering
    2. Load the employee info data from data/raw/employee_info.csv
    3. Clean both datasets
    4. Add time analysis columns
    5. Merge them
    6. Run attendance analysis
    7. Save results
    """
    # Set up logging
    logger = setup_logging()
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process key card data with date filtering')
    parser.add_argument('--last-days', type=int, default=365, 
                      help='Process only the last N days of data (default: 365)')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--all-data', action='store_true', help='Process all data regardless of date')
    args = parser.parse_args()
    
    # Start timing
    total_start_time = time.time()
    
    # Determine date range for filtering
    start_date = None
    end_date = None
    last_n_days = None
    
    if args.all_data:
        print("Processing all data (no date filtering)")
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
        print(f"Processing data from {start_date} to {end_date}")
    else:
        last_n_days = args.last_days
        print(f"Processing data from the last {last_n_days} days")

    # STEP 1: Load data with date filtering
    logger.info("STEP 1: Loading data...")
    step_start_time = time.time()
    
    key_card_df = safe_data_frame_operation(
        load_key_card_data,
        "Failed to load key card data",
        logger,
        "data/raw/key_card_access.csv", 
        start_date=start_date, 
        end_date=end_date, 
        last_n_days=last_n_days
    )
    
    if key_card_df is None:
        logger.error("Critical error: Failed to load key card data. Exiting.")
        return
        
    logger.info(f"Loaded key card data: {len(key_card_df):,} rows with {len(key_card_df.columns)} columns")
    if not key_card_df.empty:
        logger.info(f"Date range: {key_card_df['Date/time'].min()} to {key_card_df['Date/time'].max()}")

    employee_df = safe_data_frame_operation(
        load_employee_info,
        "Failed to load employee data",
        logger,
        "data/raw/employee_info.csv"
    )
    
    if employee_df is None:
        logger.error("Critical error: Failed to load employee data. Exiting.")
        return
        
    logger.info(f"Loaded employee data: {len(employee_df):,} rows with {len(employee_df.columns)} columns")    
    logger.info(f"Data loading completed in {time.time() - step_start_time:.2f} seconds")

    # STEP 2: Clean data
    logger.info("STEP 2: Cleaning data...")
    step_start_time = time.time()
    
    key_card_df = safe_data_frame_operation(
        clean_key_card_data, 
        "Failed to clean key card data", 
        logger, 
        key_card_df
    )
    
    if key_card_df is None:
        logger.error("Critical error: Failed to clean key card data. Exiting.")
        return
    
    employee_df = safe_data_frame_operation(
        clean_employee_info, 
        "Failed to clean employee data", 
        logger, 
        employee_df
    )
    
    if employee_df is None:
        logger.error("Critical error: Failed to clean employee data. Exiting.")
        return
    
    logger.info(f"Data cleaning completed in {time.time() - step_start_time:.2f} seconds")

    # STEP 3: Add time analysis columns
    logger.info("STEP 3: Adding time analysis columns...")
    step_start_time = time.time()
    
    key_card_df = safe_data_frame_operation(
        add_time_analysis_columns, 
        "Failed to add time analysis columns", 
        logger, 
        key_card_df
    )
    
    if key_card_df is None:
        logger.error("Critical error: Failed to add time analysis columns. Exiting.")
        return
    
    logger.info(f"Time analysis completed in {time.time() - step_start_time:.2f} seconds")

    # STEP 4: Merge data
    logger.info("STEP 4: Merging datasets...")
    step_start_time = time.time()
    
    combined_df = safe_data_frame_operation(
        merge_key_card_with_employee_info,
        "Failed to merge key card and employee data",
        logger,
        key_card_df, 
        employee_df
    )
    
    if combined_df is None:
        logger.error("Critical error: Failed to merge datasets. Exiting.")
        return
    
    # Clean up memory
    logger.debug("Cleaning up memory by deleting intermediate datasets")
    del key_card_df
    del employee_df
    gc.collect()
    
    logger.info(f"Data merging completed in {time.time() - step_start_time:.2f} seconds")

    # STEP 5: Final dataset information
    logger.info("STEP 5: Final dataset information")
    logger.info(f"Combined shape: {combined_df.shape[0]:,} rows, {combined_df.shape[1]} columns")

    # STEP 6: Run attendance analysis
    logger.info("STEP 6: Running attendance analysis...")
    step_start_time = time.time()
    
    # Run analyses with error handling
    attendance_table = safe_data_frame_operation(
        build_attendance_table,
        "Failed to build attendance table",
        logger,
        combined_df
    )
    
    if attendance_table is None:
        logger.error("Critical error: Failed to build attendance table. Continuing with other analyses.")
        attendance_table = pd.DataFrame()
    
    visit_counts = safe_data_frame_operation(
        calculate_visit_counts,
        "Failed to calculate visit counts",
        logger,
        combined_df
    )
    
    if visit_counts is None:
        logger.error("Failed to calculate visit counts. Continuing with other analyses.")
        visit_counts = pd.DataFrame()
    
    avg_arrival_hours = safe_data_frame_operation(
        calculate_average_arrival_hour,
        "Failed to calculate average arrival hours",
        logger,
        combined_df
    )
    
    if avg_arrival_hours is None:
        logger.error("Failed to calculate average arrival hours. Continuing with other analyses.")
        avg_arrival_hours = pd.DataFrame()

    logger.info(f"Analysis completed in {time.time() - step_start_time:.2f} seconds")

    # Create summary for logging
    logger.info("=== ATTENDANCE SUMMARY ===")
    
    if not attendance_table.empty and "employee_name" in attendance_table.columns and "days_attended" in attendance_table.columns:
        days_summary = (
            attendance_table[["employee_name", "days_attended"]]
            .drop_duplicates()
            .sort_values("days_attended", ascending=False)
        )
        logger.info(f"Total distinct employees with attendance: {len(days_summary)}")
        
        # Log summary statistics without personal details
        if not days_summary.empty:
            logger.info(f"Days attended statistics: min={days_summary['days_attended'].min()}, max={days_summary['days_attended'].max()}, avg={days_summary['days_attended'].mean():.1f}")
    else:
        logger.warning("Unable to create days summary due to missing data or columns")
        days_summary = pd.DataFrame()

    # STEP 7: Save all results
    logger.info("STEP 7: Saving results...")
    step_start_time = time.time()
    
    # Determine a suffix for the output files based on date range
    if start_date and end_date:
        suffix = f"{start_date}_to_{end_date}"
    elif last_n_days:
        suffix = f"last_{last_n_days}_days"
    else:
        suffix = "all_data"
    
    # Ensure the processed directory exists
    from pathlib import Path
    processed_dir = Path("data/processed")
    processed_dir.mkdir(exist_ok=True, parents=True)
    
    # Save all data with error handling
    try:
        # Save combined data
        output_path = f"data/processed/combined_data_{suffix}.parquet"
        combined_df.to_parquet(output_path, index=False)
        logger.info(f"Saved combined data to {output_path}")
        
        # Save analysis results
        if not attendance_table.empty:
            output_path = f"data/processed/attendance_table_{suffix}.csv"
            attendance_table.to_csv(output_path, index=False)
            logger.info(f"Saved attendance table to {output_path}")
            
        if not visit_counts.empty:
            output_path = f"data/processed/visit_counts_{suffix}.csv"
            visit_counts.to_csv(output_path, index=False)
            logger.info(f"Saved visit counts to {output_path}")
            
        if not avg_arrival_hours.empty:
            output_path = f"data/processed/avg_arrival_hours_{suffix}.csv"
            avg_arrival_hours.to_csv(output_path, index=False)
            logger.info(f"Saved average arrival hours to {output_path}")
            
        if not days_summary.empty:
            output_path = f"data/processed/days_summary_{suffix}.csv"
            days_summary.to_csv(output_path, index=False)
            logger.info(f"Saved days summary to {output_path}")
    except Exception as e:
        logger.error(f"Error saving results: {str(e)}")
    
    logger.info(f"Results saved in {time.time() - step_start_time:.2f} seconds")
    logger.info(f"All data has been saved to the data/processed directory with suffix '{suffix}'")
    logger.info(f"Total processing time: {time.time() - total_start_time:.2f} seconds")
    logger.info("To view the dashboard, run: streamlit run src/dashboard.py")

if __name__ == "__main__":
    main()
