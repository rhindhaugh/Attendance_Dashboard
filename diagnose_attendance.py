import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from src.data_ingestion import load_key_card_data, load_employee_info, load_employment_history
from src.data_cleaning import clean_key_card_data, clean_employee_info, merge_key_card_with_employee_info
from src.data_analysis import build_attendance_table, calculate_tue_thu_attendance_percentage, calculate_daily_attendance_counts

def diagnose_specific_date():
    """Run diagnostics on a specific date (December 11, 2024)."""
    print("Starting specific date diagnosis...")
    
    # Test date - December 11, 2024
    test_date = pd.Timestamp("2024-12-11")
    
    # Load ALL data
    key_card_path = Path("data/raw/key_card_access.csv")
    employee_path = Path("data/raw/employee_info.csv") 
    history_path = Path("data/raw/employment_status_history.csv")
    
    # First, load without any date filtering (representing the "year" view)
    print("\n=== LOADING FULL YEAR DATA ===")
    start_date_year = (test_date - pd.Timedelta(days=365)).strftime("%Y-%m-%d")
    end_date_year = test_date.strftime("%Y-%m-%d")
    
    key_card_year = load_key_card_data(
        str(key_card_path),
        start_date=start_date_year,
        end_date=end_date_year
    )
    employee_df = load_employee_info(str(employee_path))
    history_df = load_employment_history(str(history_path))
    
    # Process year data
    print("\nProcessing full year data...")
    key_card_year_cleaned = clean_key_card_data(key_card_year)
    max_date_year = key_card_year_cleaned['date_only'].max()
    employee_df_year = clean_employee_info(employee_df, max_date_year)
    
    year_combined = merge_key_card_with_employee_info(
        key_card_year_cleaned, 
        employee_df_year,
        history_df
    )
    
    # Build attendance table for the year
    print("\nBuilding attendance table for full year...")
    year_attendance = build_attendance_table(year_combined)
    
    # Merge attendance data back to merged df
    year_combined = year_combined.merge(
        year_attendance[['employee_id', 'date_only', 'present', 'visits']],
        on=['employee_id', 'date_only'],
        how='left'
    )
    # Fill NaN values in present column with 'No'
    year_combined['present'] = year_combined['present'].fillna('No')
    
    # Calculate attendance for December 11 from year data
    year_daily_counts = calculate_daily_attendance_counts(year_combined)
    dec11_year = year_daily_counts[year_daily_counts['date'] == test_date]
    
    # Second, load 3-month data (representing the "3 months" view)
    print("\n=== LOADING 3-MONTH DATA ===")
    start_date_3m = (test_date - pd.Timedelta(days=90)).strftime("%Y-%m-%d")
    end_date_3m = test_date.strftime("%Y-%m-%d")
    
    key_card_3m = load_key_card_data(
        str(key_card_path),
        start_date=start_date_3m,
        end_date=end_date_3m
    )
    
    # Process 3-month data
    print("\nProcessing 3-month data...")
    key_card_3m_cleaned = clean_key_card_data(key_card_3m)
    max_date_3m = key_card_3m_cleaned['date_only'].max()
    employee_df_3m = clean_employee_info(employee_df, max_date_3m)
    
    m3_combined = merge_key_card_with_employee_info(
        key_card_3m_cleaned, 
        employee_df_3m,
        history_df
    )
    
    # Build attendance table for 3 months
    print("\nBuilding attendance table for 3 months...")
    m3_attendance = build_attendance_table(m3_combined)
    
    # Merge attendance data back to merged df
    m3_combined = m3_combined.merge(
        m3_attendance[['employee_id', 'date_only', 'present', 'visits']],
        on=['employee_id', 'date_only'],
        how='left'
    )
    # Fill NaN values in present column with 'No'
    m3_combined['present'] = m3_combined['present'].fillna('No')
    
    # Calculate attendance for December 11 from 3-month data
    m3_daily_counts = calculate_daily_attendance_counts(m3_combined)
    dec11_3m = m3_daily_counts[m3_daily_counts['date'] == test_date]
    
    # Compare the results
    print("\n=== COMPARISON FOR DECEMBER 11, 2024 ===")
    
    # Check if we have data for December 11
    if dec11_year.empty:
        print("No data found for December 11, 2024 in the year view")
    
    if dec11_3m.empty:
        print("No data found for December 11, 2024 in the 3-month view")
    
    # Try to find the most recent date in both datasets
    if dec11_year.empty or dec11_3m.empty:
        print("\nAttempting to find most recent date that exists in both datasets...")
        
        all_year_dates = sorted(year_daily_counts['date'].unique())
        all_3m_dates = sorted(m3_daily_counts['date'].unique())
        
        # Find dates that exist in both datasets
        common_dates = set(all_year_dates).intersection(set(all_3m_dates))
        
        if common_dates:
            # Use the most recent common date
            most_recent_common = max(common_dates)
            print(f"Using most recent common date: {most_recent_common.strftime('%Y-%m-%d')}")
            
            dec11_year = year_daily_counts[year_daily_counts['date'] == most_recent_common]
            dec11_3m = m3_daily_counts[m3_daily_counts['date'] == most_recent_common]
    
    if not dec11_year.empty and not dec11_3m.empty:
        # Year view data
        year_pct = dec11_year['london_hybrid_ft_percentage'].values[0]
        year_count = dec11_year['london_hybrid_ft_count'].values[0]
        year_eligible = dec11_year['eligible_london_hybrid_ft'].values[0]
        
        # 3-month view data
        m3_pct = dec11_3m['london_hybrid_ft_percentage'].values[0]
        m3_count = dec11_3m['london_hybrid_ft_count'].values[0]
        m3_eligible = dec11_3m['eligible_london_hybrid_ft'].values[0]
        
        print(f"Year view - Attendance: {year_count}/{year_eligible} = {year_pct:.1f}%")
        print(f"3-mo view - Attendance: {m3_count}/{m3_eligible} = {m3_pct:.1f}%")
        print(f"Difference: {m3_pct - year_pct:.1f} percentage points")
        
        # Analyze the difference in eligible employees
        print(f"\nEligible employees difference: {m3_eligible - year_eligible}")
        
        # Analyze differences in eligible employee pools
        # Get test date (or most recent date used in the comparison)
        test_date_used = dec11_year['date'].iloc[0]
        
        # Get employees from each dataset that match the criteria
        year_eligible_emps = year_combined[
            (year_combined['date_only'] == test_date_used) & 
            (year_combined['Location'] == 'London UK') & 
            (year_combined['Working Status'] == 'Hybrid') &
            (year_combined['is_full_time'] == True)
        ]['employee_id'].unique()
        
        m3_eligible_emps = m3_combined[
            (m3_combined['date_only'] == test_date_used) & 
            (m3_combined['Location'] == 'London UK') & 
            (m3_combined['Working Status'] == 'Hybrid') &
            (m3_combined['is_full_time'] == True)
        ]['employee_id'].unique()
        
        # Find employees only in year view
        only_in_year = set(year_eligible_emps) - set(m3_eligible_emps)
        print(f"\nEmployees only in year eligible pool: {len(only_in_year)}")
        
        # Find employees only in 3-month view
        only_in_3m = set(m3_eligible_emps) - set(year_eligible_emps)
        print(f"Employees only in 3-month eligible pool: {len(only_in_3m)}")
        
        print(f"\nInvestigating differences in employee pools for date: {test_date_used.strftime('%Y-%m-%d')}")
        
        if only_in_year:
            print("\nSample of employees only in year view eligible pool:")
            for emp_id in list(only_in_year)[:5]:
                # Find this employee's data
                emp_year_data = year_combined[year_combined['employee_id'] == emp_id]
                if not emp_year_data.empty:
                    emp_data = emp_year_data.iloc[0]
                    print(f"  Employee ID: {emp_id}")
                    print(f"  Name: {emp_data.get('Last name, First name', 'Unknown')}")
                    print(f"  Location: {emp_data.get('Location', 'Unknown')}")
                    print(f"  Working Status: {emp_data.get('Working Status', 'Unknown')}")
                    print(f"  Full-Time: {emp_data.get('is_full_time', 'Unknown')}")
                    print(f"  Combined hire date: {emp_data.get('Combined hire date', 'Unknown')}")
                    print(f"  Most recent day worked: {emp_data.get('Most recent day worked', 'Unknown')}")
                    print("  ---")
        
        if only_in_3m:
            print("\nSample of employees only in 3-month view eligible pool:")
            for emp_id in list(only_in_3m)[:5]:
                # Find this employee's data
                emp_3m_data = m3_combined[m3_combined['employee_id'] == emp_id]
                if not emp_3m_data.empty:
                    emp_data = emp_3m_data.iloc[0]
                    print(f"  Employee ID: {emp_id}")
                    print(f"  Name: {emp_data.get('Last name, First name', 'Unknown')}")
                    print(f"  Location: {emp_data.get('Location', 'Unknown')}")
                    print(f"  Working Status: {emp_data.get('Working Status', 'Unknown')}")
                    print(f"  Full-Time: {emp_data.get('is_full_time', 'Unknown')}")
                    print(f"  Combined hire date: {emp_data.get('Combined hire date', 'Unknown')}")
                    print(f"  Most recent day worked: {emp_data.get('Most recent day worked', 'Unknown')}")
                    print("  ---")
        
        # Calculate the actual present employees in each view
        year_present_employees = year_combined[
            (year_combined['date_only'] == test_date) & 
            (year_combined['Location'] == 'London UK') & 
            (year_combined['Working Status'] == 'Hybrid') &
            (year_combined['is_full_time'] == True) &
            (year_combined['present'] == 'Yes')
        ]['employee_id'].nunique()
        
        m3_present_employees = m3_combined[
            (m3_combined['date_only'] == test_date) & 
            (m3_combined['Location'] == 'London UK') & 
            (m3_combined['Working Status'] == 'Hybrid') &
            (m3_combined['is_full_time'] == True) &
            (m3_combined['present'] == 'Yes')
        ]['employee_id'].nunique()
        
        print(f"\nPresent employees in year view: {year_present_employees}")
        print(f"Present employees in 3-month view: {m3_present_employees}")
        print(f"Difference: {m3_present_employees - year_present_employees}")
    
    print("\nDiagnostics complete!")

if __name__ == "__main__":
    diagnose_specific_date()