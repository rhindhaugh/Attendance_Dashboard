from src.data_ingestion import load_key_card_data, load_employee_info
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

def main():
    """
    This function will:
    1. Load the key card data from data/raw/key_card_access.csv
    2. Load the employee info data from data/raw/employee_info.csv
    3. Clean both datasets
    4. Add time analysis columns
    5. Merge them
    6. Run attendance analysis
    7. Save results
    """

    # STEP 1: Load data
    key_card_df = load_key_card_data("data/raw/key_card_access.csv")
    print("Key card dataframe columns:", key_card_df.columns.tolist())
    print("\nFirst few rows of raw key card data:")
    print(key_card_df.head())

    employee_df = load_employee_info("data/raw/employee_info.csv")
    print("\nEmployee info columns:", employee_df.columns.tolist())
    print("\nFirst few rows of raw employee data:")
    print(employee_df.head())

    # STEP 2: Clean data
    key_card_df = clean_key_card_data(key_card_df)
    employee_df = clean_employee_info(employee_df)

    # STEP 3: Add time analysis columns
    key_card_df = add_time_analysis_columns(key_card_df)

    # STEP 4: Merge data
    combined_df = merge_key_card_with_employee_info(key_card_df, employee_df)

    # STEP 5: Print shapes / head of data
    print("\nKey card (clean) shape:", key_card_df.shape)
    print("Employee info (clean) shape:", employee_df.shape)
    print("Combined shape:", combined_df.shape)

    # STEP 6: Run attendance analysis
    attendance_table = build_attendance_table(combined_df)
    visit_counts = calculate_visit_counts(combined_df)
    avg_arrival_hours = calculate_average_arrival_hour(combined_df)

    # Print summary statistics
    print("\n=== ATTENDANCE SUMMARY ===")
    days_summary = (
        attendance_table[["employee_name", "days_attended"]]
        .drop_duplicates()
        .sort_values("days_attended", ascending=False)
    )
    print("\nTotal days attended by employee:")
    print(days_summary.head(10))  # Show top 10

    print("\nAverage arrival hours:")
    print(avg_arrival_hours.head(10))  # Show top 10

    # STEP 7: Save all results
    # Save combined data
    combined_df.to_parquet("data/processed/combined_data.parquet", index=False)
    combined_df.to_csv("data/processed/combined_data.csv", index=False)
    
    # Save analysis results
    attendance_table.to_csv("data/processed/attendance_table.csv", index=False)
    visit_counts.to_csv("data/processed/visit_counts.csv", index=False)
    avg_arrival_hours.to_csv("data/processed/avg_arrival_hours.csv", index=False)
    days_summary.to_csv("data/processed/days_summary.csv", index=False)

    print("\nAll data has been saved to the data/processed directory.")
    print("\nTo view the dashboard, run: streamlit run src/dashboard.py")

if __name__ == "__main__":
    main()
