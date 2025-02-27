import pandas as pd

def calculate_daily_attendance_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily attendance counts split by employee type."""
    # Calculate attendance for each day
    daily_counts = []
    
    for date in sorted(df['date_only'].unique()):
        # Define date_mask first so it's available for both debug and regular code
        date_mask = df['date_only'] == date
        
        # Debug code for September 12, 2024
        if pd.Timestamp(date).strftime('%d/%m/%Y') == '12/09/2024':
            print("\n===== DEBUGGING SEPTEMBER 12, 2024 =====")
            
            # Get all employees present that day regardless of other criteria
            all_present = df[date_mask & (df['present'] == 'Yes')]['employee_id'].unique()
            print(f"Total unique employees present on Sep 12: {len(all_present)}")
            
            # Check employment dates
            active_mask = (
                (pd.to_datetime(df['Combined hire date']) <= date) & 
                ((df['Most recent day worked'].isna()) | (pd.to_datetime(df['Most recent day worked']) >= date))
            )
            
            active_present = df[date_mask & active_mask & (df['present'] == 'Yes')]['employee_id'].unique()
            print(f"Employees present and active based on hire/departure dates: {len(active_present)}")
            
            # How many were excluded due to employment dates?
            employment_excluded = set(all_present) - set(active_present)
            print(f"Excluded due to employment dates: {len(employment_excluded)}")
            
            # Show details of excluded employees
            if employment_excluded:
                print("\nEmployees excluded due to employment dates:")
                for emp_id in employment_excluded:
                    emp_data = df[(df['employee_id'] == emp_id) & date_mask]
                    if not emp_data.empty:
                        row = emp_data.iloc[0]
                        name = row['Last name, First name'] if 'Last name, First name' in row else 'Unknown'
                        hire_date = row['Combined hire date'] if 'Combined hire date' in row else 'Unknown'
                        last_day = row['Most recent day worked'] if 'Most recent day worked' in row else 'Unknown'
                        print(f"  Employee ID: {emp_id}")
                        print(f"  Name: {name}")
                        print(f"  Hire date: {hire_date}")
                        print(f"  Last day worked: {last_day}")
                        print("  ---")
            
            # Check categorization
            london_hybrid_ft_mask = (
                (df['Location'] == 'London UK') & 
                (df['Working Status'] == 'Hybrid') &
                (df['is_full_time'] == True)
            )
            
            london_hybrid_ft_present = df[
                date_mask & active_mask & london_hybrid_ft_mask & (df['present'] == 'Yes')
            ]['employee_id'].unique()
            print(f"\nLondon, Hybrid, Full-Time employees counted: {len(london_hybrid_ft_present)}")
            
            others_present = df[
                date_mask & active_mask & ~london_hybrid_ft_mask & (df['present'] == 'Yes')
            ]['employee_id'].unique()
            print(f"Other employees counted: {len(others_present)}")
            
            # Check for NaN values in relevant columns
            nan_location = df[date_mask & active_mask & (df['present'] == 'Yes') & df['Location'].isna()]['employee_id'].unique()
            nan_working = df[date_mask & active_mask & (df['present'] == 'Yes') & df['Working Status'].isna()]['employee_id'].unique()
            nan_full_time = df[date_mask & active_mask & (df['present'] == 'Yes') & df['is_full_time'].isna()]['employee_id'].unique()
            print(f"Employees with NaN Location: {len(nan_location)}")
            print(f"Employees with NaN Working Status: {len(nan_working)}")
            print(f"Employees with NaN is_full_time: {len(nan_full_time)}")
            
            # Total employees that should be counted
            should_be_counted = set(london_hybrid_ft_present) | set(others_present)
            missing = set(active_present) - should_be_counted
            print(f"MISSING EMPLOYEES: {len(missing)}")
            if missing:
                print("\nSample of missing employees:")
                for emp_id in list(missing)[:5]:
                    emp_rows = df[(df['employee_id'] == emp_id) & date_mask]
                    if not emp_rows.empty:
                        emp_row = emp_rows.iloc[0]
                        print(f"  ID: {emp_id}, Name: {emp_row.get('Last name, First name', 'N/A')}")
                        print(f"  Location: {emp_row.get('Location', 'N/A')}, Working Status: {emp_row.get('Working Status', 'N/A')}")
                        print(f"  Full-Time: {emp_row.get('is_full_time', 'N/A')}")
                        print(f"  Present: {emp_row.get('present', 'N/A')}, Combined hire date: {emp_row.get('Combined hire date', 'N/A')}")
                        print(f"  Most recent day worked: {emp_row.get('Most recent day worked', 'N/A')}")
                        print("  ---")
        
        # Continue with the original function calculation
        # Consider employment dates
        active_mask = (
            (pd.to_datetime(df['Combined hire date']) <= date) & 
            ((df['Most recent day worked'].isna()) | (pd.to_datetime(df['Most recent day worked']) >= date))
        )
        
        # London, Hybrid, Full-Time mask
        london_hybrid_ft_mask = (
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid') &
            (df['is_full_time'] == True)
        )
        
        # Count London, Hybrid, Full-Time who were present
        london_hybrid_ft_count = df[
            date_mask & active_mask & london_hybrid_ft_mask & (df['present'] == 'Yes')
        ]['employee_id'].nunique()
        
        # Count others who were present
        others_count = df[
            date_mask & active_mask & ~london_hybrid_ft_mask & (df['present'] == 'Yes')
        ]['employee_id'].nunique()
        
        # Get total eligible London, Hybrid, Full-Time employees for that date
        total_eligible_london_hybrid_ft = df[
            active_mask & london_hybrid_ft_mask
        ]['employee_id'].nunique()
        
        # Calculate percentage based on eligible employees
        attendance_percentage = (
            (london_hybrid_ft_count / total_eligible_london_hybrid_ft * 100)
            if total_eligible_london_hybrid_ft > 0 else 0
        )
        
        daily_counts.append({
            'date': date,
            'day_of_week': pd.Timestamp(date).strftime('%A'),
            'london_hybrid_ft_count': london_hybrid_ft_count,
            'other_count': others_count,
            'eligible_london_hybrid_ft': total_eligible_london_hybrid_ft,
            'london_hybrid_ft_percentage': round(attendance_percentage, 1),
            'total_attendance': london_hybrid_ft_count + others_count
        })
    
    return pd.DataFrame(daily_counts)

def calculate_weekly_attendance_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate weekly attendance counts split by employee type.
    Only considers Tuesday, Wednesday, and Thursday.
    """
    df = df.copy()
    
    # Add week start date (Monday)
    df['week_start'] = df['date_only'] - pd.to_timedelta(df['date_only'].dt.dayofweek, unit='d')
    
    weekly_counts = []
    for week_start in sorted(df['week_start'].unique()):
        week_end = week_start + pd.Timedelta(days=6)
        
        # Updated week_mask to only include Tue-Thu
        week_mask = (
            (df['date_only'] >= week_start) & 
            (df['date_only'] <= week_end) &
            (df['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday']))
        )
        
        # Get daily eligible counts for Tue-Thu
        daily_eligible = []
        for date in pd.date_range(week_start, week_end):
            if date.strftime('%A') in ['Tuesday', 'Wednesday', 'Thursday']:
                # Consider employment dates
                active_mask = (
                    (pd.to_datetime(df['Combined hire date']) <= date) &
                    (
                        (df['Most recent day worked'].isna()) |
                        (pd.to_datetime(df['Most recent day worked']) >= date)
                    )
                )
                
                # London, Hybrid, Full-Time mask
                london_hybrid_ft_mask = (
                    (df['Location'] == 'London UK') & 
                    (df['Working Status'] == 'Hybrid') &
                    (df['is_full_time'] == True)
                )
                
                # Count eligible London, Hybrid, Full-Time employees for this day
                eligible_count = df[
                    active_mask & london_hybrid_ft_mask
                ]['employee_id'].nunique()
                
                daily_eligible.append(eligible_count)
        
        # Calculate average eligible employees across Tue-Thu
        avg_eligible = round(sum(daily_eligible) / len(daily_eligible), 1) if daily_eligible else 0
        
        # Consider employment dates for the week
        active_mask = (
            (pd.to_datetime(df['Combined hire date']) <= week_end) &
            (
                (df['Most recent day worked'].isna()) |
                (pd.to_datetime(df['Most recent day worked']) >= week_start)
            )
        )
        
        # London, Hybrid, Full-Time mask
        london_hybrid_ft_mask = (
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid') &
            (df['is_full_time'] == True)
        )
        
        # Calculate daily attendance for London, Hybrid, Full-Time
        london_hybrid_ft_daily = df[
            week_mask & 
            active_mask &
            london_hybrid_ft_mask & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique()
        
        # Calculate daily attendance for others
        others_daily = df[
            week_mask & 
            active_mask &
            ~london_hybrid_ft_mask & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique()
        
        # Calculate averages
        london_hybrid_ft_avg = london_hybrid_ft_daily.mean() if not london_hybrid_ft_daily.empty else 0
        others_avg = others_daily.mean() if not others_daily.empty else 0
        
        # Calculate attendance percentage using average eligible employees
        attendance_percentage = (
            (london_hybrid_ft_avg / avg_eligible * 100)
            if avg_eligible > 0 else 0
        )
        
        weekly_counts.append({
            'week_start': week_start,
            'london_hybrid_ft_avg': round(london_hybrid_ft_avg, 1),
            'other_avg': round(others_avg, 1),
            'avg_eligible_london_hybrid_ft': avg_eligible,
            'london_hybrid_ft_percentage': round(attendance_percentage, 1),
            'total_avg_attendance': round(london_hybrid_ft_avg + others_avg, 1)
        })
    
    return pd.DataFrame(weekly_counts)