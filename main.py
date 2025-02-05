from src.data_ingestion import load_key_card_data, load_employee_info
from src.data_cleaning import (
    clean_key_card_data,
    clean_employee_info,
    merge_key_card_with_employee_info,
    add_time_analysis_columns
)

def main():
    """
    This function will:
    1. Load the key card data from data/raw/key_card_access.csv
    2. Load the employee info data from data/raw/employee_info.csv
    3. Clean both datasets
    4. Add time analysis columns
    5. Merge them
    6. Print a summary of the results
    7. Write out the combined data
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

    # Print the first few rows to confirm
    print("\n=== COMBINED DF (head) ===")
    print(combined_df.head())

    # STEP 6: Save the combined data
    combined_df.to_parquet("data/processed/combined_data.parquet", index=False)
    # Also save as CSV for easier viewing if needed
    combined_df.to_csv("data/processed/combined_data.csv", index=False)

if __name__ == "__main__":
    main()
