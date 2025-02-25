import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from src.data_ingestion import load_key_card_data, load_employee_info
from src.data_cleaning import clean_key_card_data, clean_employee_info, merge_key_card_with_employee_info
from src.data_analysis import build_attendance_table, calculate_tue_thu_attendance_percentage

def diagnose_november_attendance():
    """Run diagnostics on November 2024 attendance data."""
    print("Starting attendance diagnosis...")
    
    # Test date range
    month = 11
    year = 2024
    
    # Load data using both methods
    print("\n=== TESTING OLD VS NEW DATA LOADING ===")
    
    # Method 1: Load everything
    key_card_path = Path("data/raw/key_card_access.csv")
    original_df = pd.read_csv(key_card_path)
    print(f"Total rows in original CSV: {len(original_df):,}")
    
    # Parse dates
    original_df['Date/time'] = pd.to_datetime(original_df['Date/time'], dayfirst=True)
    
    # Filter for November
    start_date = pd.Timestamp(f"{year}-{month:02d}-01")
    end_date = pd.Timestamp(f"{year}-{month+1:02d}-01") if month < 12 else pd.Timestamp(f"{year+1}-01-01")
    end_date = end_date - pd.Timedelta(days=1)
    
    nov_original = original_df[
        (original_df['Date/time'] >= start_date) &
        (original_df['Date/time'] <= end_date)
    ]
    
    print(f"November {year} records (original method): {len(nov_original):,}")
    
    # Method 2: Use optimized load with specific date
    optimized_df = load_key_card_data(
        str(key_card_path),
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d")
    )
    
    print(f"November {year} records (optimized method): {len(optimized_df):,}")
    
    if len(nov_original) != len(optimized_df):
        print(f"DISCREPANCY DETECTED: {abs(len(nov_original) - len(optimized_df)):,} rows difference")
        
        # Check for differences
        if 'Date/time' in optimized_df.columns:
            optimized_df['Date/time'] = pd.to_datetime(optimized_df['Date/time'], dayfirst=True)
            
            # Compare date ranges
            print(f"Original date range: {nov_original['Date/time'].min()} to {nov_original['Date/time'].max()}")
            print(f"Optimized date range: {optimized_df['Date/time'].min()} to {optimized_df['Date/time'].max()}")
    
    # Process both datasets and compare attendance percentages
    print("\n=== COMPARING ATTENDANCE PERCENTAGES ===")
    
    # Load employee data
    employee_path = Path("data/raw/employee_info.csv") 
    employee_df = load_employee_info(str(employee_path))
    employee_df = clean_employee_info(employee_df)
    
    # Process original data
    original_cleaned = clean_key_card_data(nov_original)
    original_combined = merge_key_card_with_employee_info(original_cleaned, employee_df)
    original_attendance = build_attendance_table(original_combined)
    
    # Add debug print to check columns before percentage calculation
    print("\nColumns in original_attendance:")
    print(original_attendance.columns.tolist())
    
    # Add debug print to check first few rows
    print("\nFirst few rows of original_attendance:")
    print(original_attendance.head())
    
    original_pct = calculate_tue_thu_attendance_percentage(original_attendance)

    # Process optimized data
    optimized_cleaned = clean_key_card_data(optimized_df)
    optimized_combined = merge_key_card_with_employee_info(optimized_cleaned, employee_df)
    optimized_attendance = build_attendance_table(optimized_combined)
    optimized_pct = calculate_tue_thu_attendance_percentage(optimized_attendance)
    
    print("\nAttendance Comparison:")
    print(f"Original: {len(original_attendance):,} employee-day combinations")
    print(f"Optimized: {len(optimized_attendance):,} employee-day combinations")
    
    if not original_pct.empty and not optimized_pct.empty:
        # Filter for November
        original_nov = original_pct[
            (original_pct['date'] >= start_date) &
            (original_pct['date'] <= end_date)
        ]
        
        optimized_nov = optimized_pct[
            (optimized_pct['date'] >= start_date) &
            (optimized_pct['date'] <= end_date)
        ]
        
        print("\nNovember Attendance Percentages:")
        print(f"Original method average: {original_nov['percentage'].mean():.1f}%")
        print(f"Optimized method average: {optimized_nov['percentage'].mean():.1f}%")
        
        # If there's a significant difference, show day-by-day comparison
        if abs(original_nov['percentage'].mean() - optimized_nov['percentage'].mean()) > 1.0:
            print("\nDay-by-day comparison:")
            comparison = pd.merge(
                original_nov[['date', 'percentage']].rename(columns={'percentage': 'original_pct'}),
                optimized_nov[['date', 'percentage']].rename(columns={'percentage': 'optimized_pct'}),
                on='date',
                how='outer'
            )
            comparison['difference'] = comparison['original_pct'] - comparison['optimized_pct']
            comparison['date_str'] = comparison['date'].dt.strftime('%Y-%m-%d')
            print(comparison[['date_str', 'original_pct', 'optimized_pct', 'difference']].sort_values('date'))
    
    print("\nDiagnostics complete!")

if __name__ == "__main__":
    diagnose_november_attendance()