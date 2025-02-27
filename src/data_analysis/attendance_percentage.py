import pandas as pd

def calculate_daily_attendance_percentage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the daily attendance percentage for London, Hybrid, Full-Time employees.
    Only counts employees who:
    - Are based in London UK
    - Have hybrid working status
    - Are full-time employees on that date
    - Were employed on that date (after hire date, before resignation)
    """
    # Get all unique dates and sort them
    all_dates = sorted(df['date_only'].unique())
    
    # Initialize results
    daily_attendance = []
    
    for date in all_dates:
        # Filter for employees who were employed on this date
        active_mask = (
            (pd.to_datetime(df['Combined hire date']) <= date) &
            (
                (df['Most recent day worked'].isna()) |  # Still employed
                (pd.to_datetime(df['Most recent day worked']) >= date)  # Not yet left
            )
        )
        
        # Filter for London, Hybrid, Full-Time employees
        location_mask = (df['Location'] == 'London UK')
        working_mask = (df['Working Status'] == 'Hybrid')
        full_time_mask = (df['is_full_time'] == True)
        
        # Combined mask for London, Hybrid, Full-Time
        london_hybrid_ft_mask = location_mask & working_mask & full_time_mask
        
        # Get total eligible employees for this date
        eligible_employees = df[
            active_mask & london_hybrid_ft_mask
        ]['employee_id'].nunique()
        
        # Get employees who were present
        present_employees = df[
            (df['date_only'] == date) &
            active_mask & 
            london_hybrid_ft_mask &
            (df['is_present'] == True)
        ]['employee_id'].nunique()
        
        # Calculate percentage
        if eligible_employees > 0:
            percentage = (present_employees / eligible_employees) * 100
        else:
            percentage = 0
        
        daily_attendance.append({
            'date': date,
            'total_eligible': eligible_employees,
            'total_present': present_employees,
            'percentage': round(percentage, 1)
        })
    
    return pd.DataFrame(daily_attendance)

def calculate_weekly_attendance_percentage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate weekly attendance percentage, considering Tuesday-Thursday.
    Uses the attendance table (one row per employee per day).
    FIXED: Now uses the same calculation logic as daily attendance for consistent results.
    """
    # Ensure we're working with datetime
    df = df.copy()
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Filter for office days (Tuesday-Thursday) and calculate week start
    office_days = df[df['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])]
    office_days['week_commencing'] = office_days['date_only'] - pd.to_timedelta(
        office_days['date_only'].dt.dayofweek, unit='d')
    
    # Check if we have full employee info available for consistent calculations
    has_full_employee_info = hasattr(df, 'attrs') and 'full_employee_info' in df.attrs
    
    result = []
    for week in sorted(office_days['week_commencing'].unique()):
        week_end = week + pd.Timedelta(days=6)
        
        # Get daily attendance and eligible counts for Tue-Thu
        daily_eligible = []
        daily_attendance = []
        
        for date in pd.date_range(week, week_end):
            if date.strftime('%A') in ['Tuesday', 'Wednesday', 'Thursday']:
                # Consider employment dates - exactly like daily calculation
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
                
                # Get daily eligible count using the same method as daily calculation
                if has_full_employee_info:
                    # Get full employee info DataFrame 
                    full_emp_df = df.attrs['full_employee_info']
                    
                    # Apply employment date filter to full dataset
                    active_mask_full = (
                        (pd.to_datetime(full_emp_df['Combined hire date']) <= date) &
                        ((full_emp_df['Most recent day worked'].isna()) | 
                         (pd.to_datetime(full_emp_df['Most recent day worked']) >= date))
                    )
                    
                    # Apply London, Hybrid, Full-Time filter to full dataset
                    london_hybrid_ft_mask_full = (
                        (full_emp_df['Location'] == 'London UK') & 
                        (full_emp_df['Working Status'] == 'Hybrid') &
                        (full_emp_df['is_full_time'] == True)
                    )
                    
                    # Count eligible employees from full employee pool
                    eligible_count = full_emp_df[
                        active_mask_full & london_hybrid_ft_mask_full
                    ]['employee_id'].nunique()
                else:
                    # Count eligible from current data
                    eligible_count = df[
                        active_mask & london_hybrid_ft_mask
                    ]['employee_id'].nunique()
                
                # Count attendance for this day
                date_mask = (df['date_only'] == date)
                attendance_count = df[
                    date_mask &
                    active_mask & 
                    london_hybrid_ft_mask &
                    (df['is_present'] == True)
                ]['employee_id'].nunique()
                
                daily_eligible.append(eligible_count)
                daily_attendance.append(attendance_count)
        
        if not daily_eligible:
            continue
            
        # Calculate average eligible and average attendance
        avg_eligible = sum(daily_eligible) / len(daily_eligible) if daily_eligible else 0
        avg_attendance = sum(daily_attendance) / len(daily_attendance) if daily_attendance else 0
        total_attendance = sum(daily_attendance)
        total_possible = sum(daily_eligible)
        
        # Calculate attendance percentage
        attendance_percentage = (avg_attendance / avg_eligible * 100) if avg_eligible > 0 else 0
        
        result.append({
            'week_commencing': week,
            'avg_attendance': round(avg_attendance, 1),
            'avg_eligible': round(avg_eligible, 1),
            'total_attendance': total_attendance,
            'total_possible_days': total_possible,
            'attendance_percentage': round(attendance_percentage, 1)
        })
    
    return pd.DataFrame(result).sort_values('week_commencing')

def calculate_tue_thu_attendance_percentage(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily attendance percentage, excluding Mon/Fri."""
    df = df.copy()
    
    # Filter for Tue-Thu
    tue_thu_mask = df['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])
    df = df[tue_thu_mask]
    
    # Get all unique dates and sort them
    all_dates = sorted(df['date_only'].unique())
    
    # Check if we have full employee info available for consistent calculations
    has_full_employee_info = hasattr(df, 'attrs') and 'full_employee_info' in df.attrs
    
    daily_attendance = []
    for date in all_dates:
        # Step 1: Calculate present employees (only counts who actually were present)
        active_mask = (
            (pd.to_datetime(df['Combined hire date']) <= date) &
            (
                (df['Most recent day worked'].isna()) |
                (pd.to_datetime(df['Most recent day worked']) >= date)
            )
        )
        
        # Filter for London, Hybrid, Full-Time employees
        location_mask = (df['Location'] == 'London UK')
        working_mask = (df['Working Status'] == 'Hybrid')
        full_time_mask = (df['is_full_time'] == True)
        
        # Combined mask for London, Hybrid, Full-Time
        london_hybrid_ft_mask = location_mask & working_mask & full_time_mask
        
        # Count employees present on this date
        present_employees = df[
            (df['date_only'] == date) &
            active_mask & 
            london_hybrid_ft_mask &
            (df['is_present'] == True)
        ]['employee_id'].nunique()
        
        # Step 2: Get eligible employee count from full employee pool if available
        if has_full_employee_info:
            # Get full employee info DataFrame
            full_emp_df = df.attrs['full_employee_info']
            
            # Apply employment date filter
            active_mask_full = (
                (pd.to_datetime(full_emp_df['Combined hire date']) <= date) &
                ((full_emp_df['Most recent day worked'].isna()) | 
                 (pd.to_datetime(full_emp_df['Most recent day worked']) >= date))
            )
            
            # Apply London, Hybrid, Full-Time filter
            london_hybrid_ft_mask_full = (
                (full_emp_df['Location'] == 'London UK') & 
                (full_emp_df['Working Status'] == 'Hybrid') &
                (full_emp_df['is_full_time'] == True)
            )
            
            # Count eligible employees from full employee pool
            eligible_employees = full_emp_df[
                active_mask_full & london_hybrid_ft_mask_full
            ]['employee_id'].nunique()
        else:
            # Calculate from current data if full employee info not available
            eligible_employees = df[active_mask & london_hybrid_ft_mask]['employee_id'].nunique()
        
        # Calculate percentage
        percentage = (present_employees / eligible_employees * 100) if eligible_employees > 0 else 0
        
        daily_attendance.append({
            'date': date,
            'total_eligible': eligible_employees,
            'total_present': present_employees,
            'percentage': round(percentage, 1)
        })
    
    return pd.DataFrame(daily_attendance)