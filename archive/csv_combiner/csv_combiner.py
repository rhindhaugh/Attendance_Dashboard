#!/usr/bin/env python3
import os
import glob
import pandas as pd

def combine_csv_files(input_dir, output_file, subset_cols=None):
    """
    Combines all CSV files in input_dir into one DataFrame,
    removes duplicate rows, sorts by date/time (newest first),
    and saves the result to output_file, preserving the original format.
    
    Parameters:
      - input_dir: str, directory where the CSV files are located.
      - output_file: str, path to the output CSV file.
      - subset_cols: list of str, optional. If provided, duplicates will be
                     dropped based on these columns.
    """
    # Find all CSV files in the input directory
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    # Define dtypes for columns to prevent mixed types
    dtype_dict = {
        'User': str,
        'Event': str,
        'Where': str,
        'Date/time': str,
        'Card Number': str,  # Column 5
        'Door': str,         # Column 6
        'Employee #': str
    }
    
    # Read each CSV file into a DataFrame
    df_list = []
    for file in csv_files:
        try:
            df = pd.read_csv(file, dtype=dtype_dict)
            df_list.append(df)
            print(f"Loaded {file} with {len(df)} rows")
        except Exception as e:
            print(f"Error reading {file}: {e}")
    
    # Concatenate all DataFrames
    combined_df = pd.concat(df_list, ignore_index=True)
    print(f"\nCombined DataFrame has {len(combined_df)} rows before deduplication.")
    
    # Drop duplicates (using all columns or a subset if provided)
    if subset_cols:
        combined_df = combined_df.drop_duplicates(subset=subset_cols)
    else:
        combined_df = combined_df.drop_duplicates()
    print(f"Combined DataFrame has {len(combined_df)} rows after deduplication.")
    
    # Convert 'Date/time' to datetime for proper sorting
    # The format matches your data: "31/07/2023 22:48:58"
    combined_df['datetime_for_sorting'] = pd.to_datetime(combined_df['Date/time'], format="%d/%m/%Y %H:%M:%S")
    
    # Sort by Date/time in descending order (newest first)
    combined_df = combined_df.sort_values(by='datetime_for_sorting', ascending=False)
    
    # Remove the temporary sorting column
    combined_df = combined_df.drop('datetime_for_sorting', axis=1)
    
    # Save the combined DataFrame to the output CSV file
    combined_df.to_csv(output_file, index=False)
    print(f"\nCombined CSV saved to {output_file}")

if __name__ == "__main__":
    # Define the input directory (where individual CSV files are stored)
    # Using absolute path to avoid relative path issues
    input_directory = "/Users/rob.hindhaugh/Documents/GitHub/Attendance_Dashboard/data/raw/csv_combiner/input_files"
    
    # Define the output CSV path, which is the file your project currently uses.
    output_csv = "/Users/rob.hindhaugh/Documents/GitHub/Attendance_Dashboard/data/raw/key_card_access.csv"
    
    # Optionally define columns to use for deduplication.
    # For example, if 'Date/time' and 'User' uniquely identify a record:
    dedupe_subset = ['Date/time', 'User']
    
    combine_csv_files(input_directory, output_csv, subset_cols=dedupe_subset)