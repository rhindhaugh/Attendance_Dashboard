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
            (df['present'] == 'Yes')
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
    """
    # Ensure we're working with datetime
    df = df.copy()
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Filter for office days and create week_commencing
    office_days = df[df['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])]
    office_days['week_commencing'] = office_days['date_only'] - pd.to_timedelta(
        office_days['date_only'].dt.dayofweek, unit='d')
    
    result = []
    for week in sorted(office_days['week_commencing'].unique()):
        week_end = week + pd.Timedelta(days=6)
        
        # Get eligible employees for this week (London, Hybrid, Full-Time)
        eligible_employees = df[
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid') &
            (df['is_full_time'] == True) &
            (pd.to_datetime(df['Combined hire date']) <= week_end) &
            ((pd.to_datetime(df['Most recent day worked']) >= week) | 
             (df['Status'] == 'Active'))
        ].drop_duplicates('employee_id')
        
        if len(eligible_employees) == 0:
            continue
        
        # For each eligible employee, count their attendance days this week
        total_attendance = 0
        for _, emp in eligible_employees.iterrows():
            days_attended = office_days[
                (office_days['week_commencing'] == week) &
                (office_days['employee_id'] == emp['employee_id']) &
                (office_days['present'] == 'Yes')
            ]['date_only'].nunique()
            total_attendance += days_attended
        
        # Total possible days is (eligible employees × 3 days)
        total_possible_days = len(eligible_employees) * 3
        attendance_percentage = (total_attendance / total_possible_days * 100)
        
        result.append({
            'week_commencing': week,
            'days_attended': total_attendance,
            'total_possible_days': total_possible_days,
            'attendance_percentage': attendance_percentage
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
            (df['present'] == 'Yes')
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