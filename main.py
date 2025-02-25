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
import argparse
from datetime import datetime, timedelta
import time
import gc

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
    print("\nSTEP 1: Loading data...")
    step_start_time = time.time()
    
    key_card_df = load_key_card_data(
        "data/raw/key_card_access.csv", 
        start_date=start_date, 
        end_date=end_date, 
        last_n_days=last_n_days
    )
    print("Key card dataframe columns:", key_card_df.columns.tolist())
    print(f"Loaded key card data: {len(key_card_df):,} rows")
    if not key_card_df.empty:
        print(f"Date range: {key_card_df['Date/time'].min()} to {key_card_df['Date/time'].max()}")

    employee_df = load_employee_info("data/raw/employee_info.csv")
    print("\nEmployee info columns:", employee_df.columns.tolist())
    print(f"Loaded employee data: {len(employee_df):,} rows")
    
    print(f"Data loading completed in {time.time() - step_start_time:.2f} seconds")

    # STEP 2: Clean data
    print("\nSTEP 2: Cleaning data...")
    step_start_time = time.time()
    
    key_card_df = clean_key_card_data(key_card_df)
    employee_df = clean_employee_info(employee_df)
    
    print(f"Data cleaning completed in {time.time() - step_start_time:.2f} seconds")

    # STEP 3: Add time analysis columns
    print("\nSTEP 3: Adding time analysis columns...")
    step_start_time = time.time()
    
    key_card_df = add_time_analysis_columns(key_card_df)
    
    print(f"Time analysis completed in {time.time() - step_start_time:.2f} seconds")

    # STEP 4: Merge data
    print("\nSTEP 4: Merging datasets...")
    step_start_time = time.time()
    
    combined_df = merge_key_card_with_employee_info(key_card_df, employee_df)
    
    # Clean up memory
    del key_card_df
    del employee_df
    gc.collect()
    
    print(f"Data merging completed in {time.time() - step_start_time:.2f} seconds")

    # STEP 5: Print shapes / head of data
    print("\nSTEP 5: Final dataset information")
    print(f"Combined shape: {combined_df.shape[0]:,} rows, {combined_df.shape[1]} columns")

    # STEP 6: Run attendance analysis
    print("\nSTEP 6: Running attendance analysis...")
    step_start_time = time.time()
    
    attendance_table = build_attendance_table(combined_df)
    visit_counts = calculate_visit_counts(combined_df)
    avg_arrival_hours = calculate_average_arrival_hour(combined_df)

    print(f"Analysis completed in {time.time() - step_start_time:.2f} seconds")

    # Print summary statistics
    print("\n=== ATTENDANCE SUMMARY ===")
    days_summary = (
        attendance_table[["employee_name", "days_attended"]]
        .drop_duplicates()
        .sort_values("days_attended", ascending=False)
    )
    print("\nTotal days attended by employee (top 10):")
    print(days_summary.head(10))

    print("\nAverage arrival hours (top 10):")
    print(avg_arrival_hours.head(10))

    # STEP 7: Save all results
    print("\nSTEP 7: Saving results...")
    step_start_time = time.time()
    
    # Determine a suffix for the output files based on date range
    if start_date and end_date:
        suffix = f"{start_date}_to_{end_date}"
    elif last_n_days:
        suffix = f"last_{last_n_days}_days"
    else:
        suffix = "all_data"
    
    # Save combined data with the suffix
    combined_df.to_parquet(f"data/processed/combined_data_{suffix}.parquet", index=False)
    
    # Save analysis results with the suffix
    attendance_table.to_csv(f"data/processed/attendance_table_{suffix}.csv", index=False)
    visit_counts.to_csv(f"data/processed/visit_counts_{suffix}.csv", index=False)
    avg_arrival_hours.to_csv(f"data/processed/avg_arrival_hours_{suffix}.csv", index=False)
    days_summary.to_csv(f"data/processed/days_summary_{suffix}.csv", index=False)

    print(f"Results saved in {time.time() - step_start_time:.2f} seconds")
    print(f"\nAll data has been saved to the data/processed directory with suffix '{suffix}'")
    print(f"Total processing time: {time.time() - total_start_time:.2f} seconds")
    print("\nTo view the dashboard, run: streamlit run src/dashboard.py")

if __name__ == "__main__":
    main()
