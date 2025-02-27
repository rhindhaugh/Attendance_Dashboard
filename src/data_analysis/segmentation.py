import pandas as pd

def calculate_attendance_by_weekday(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate attendance numbers by day of week.
    
    Args:
        df: Combined dataframe with employee and attendance data
        
    Returns:
        DataFrame with attendance by weekday
    """
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekday_counts = df.groupby('day_of_week').apply(
        lambda x: pd.Series({
            'london_hybrid_ft_count': sum((x['Location'] == 'London UK') & 
                                       (x['Working Status'] == 'Hybrid') &
                                       (x['is_full_time'] == True) &
                                       (x['present'] == 'Yes')),
            'other_count': sum(~((x['Location'] == 'London UK') & 
                               (x['Working Status'] == 'Hybrid') &
                               (x['is_full_time'] == True)) &
                             (x['present'] == 'Yes'))
        })
    ).reset_index()
    
    weekday_counts['day_of_week'] = pd.Categorical(
        weekday_counts['day_of_week'], 
        categories=weekday_order, 
        ordered=True
    )
    return weekday_counts.sort_values('day_of_week')

def calculate_attendance_by_division(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate attendance numbers and percentages by division.
    Counts unique employee-days for accurate attendance tracking.
    """
    # Get unique divisions, filtering out NaN values
    unique_divisions = [d for d in df['Division'].unique() if pd.notna(d)]
    unique_divisions.sort()  # Sort alphabetically
    
    result = []
    for division in unique_divisions:
        # Get division employees who are London, Hybrid, Full-Time
        division_employees = df[
            (df['Division'] == division) & 
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid') &
            (df['is_full_time'] == True)
        ].drop_duplicates('employee_id')
        
        if len(division_employees) == 0:
            continue
        
        # Count unique employee-days of attendance
        attendance_days = df[
            (df['Division'] == division) & 
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid') &
            (df['is_full_time'] == True) & 
            (df['present'] == 'Yes')
        ].groupby('employee_id')['date_only'].nunique().sum()
        
        # Calculate total possible days for each employee based on their employment period
        total_possible_days = 0
        for _, emp in division_employees.iterrows():
            hire_date = pd.to_datetime(emp['Combined hire date'])
            last_day = pd.to_datetime(emp['Most recent day worked'])
            
            # Get all dates in the dataset
            all_dates = pd.to_datetime(df['date_only'].unique())
            
            # Filter dates to employment period
            valid_dates = all_dates[
                (all_dates >= hire_date) & 
                ((all_dates <= last_day) | (emp['Status'] == 'Active'))
            ]
            
            total_possible_days += len(valid_dates)
        
        # Calculate percentage
        attendance_percentage = (attendance_days / total_possible_days * 100) if total_possible_days > 0 else 0
        
        result.append({
            'division': division,
            'attendance_days': attendance_days,
            'total_possible_days': total_possible_days,
            'attendance_percentage': attendance_percentage
        })
    
    return pd.DataFrame(result)

def calculate_division_attendance_tue_thu(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate average daily attendance (%) by division, only for Tuesdays, Wednesdays and Thursdays.
    
    Args:
        df: Combined dataframe with employee and attendance data
        
    Returns:
        DataFrame with division, attendance_percentage 
    """
    # Filter for only Tuesday, Wednesday, Thursday
    tue_thu_mask = df['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])
    tue_thu_df = df[tue_thu_mask]
    
    # Get unique divisions, filtering out NaN values
    unique_divisions = [d for d in tue_thu_df['Division'].unique() if pd.notna(d)]
    unique_divisions.sort()  # Sort alphabetically
    
    result = []
    for division in unique_divisions:
        # Get eligible employees (London, Hybrid, Full-Time in this division)
        division_employees = tue_thu_df[
            (tue_thu_df['Division'] == division) & 
            (tue_thu_df['Location'] == 'London UK') & 
            (tue_thu_df['Working Status'] == 'Hybrid') &
            (tue_thu_df['is_full_time'] == True)
        ]['employee_id'].nunique()
        
        if division_employees == 0:
            continue
        
        # Count attendance for Tuesday-Thursday
        division_attendance = tue_thu_df[
            (tue_thu_df['Division'] == division) & 
            (tue_thu_df['Location'] == 'London UK') & 
            (tue_thu_df['Working Status'] == 'Hybrid') &
            (tue_thu_df['is_full_time'] == True) & 
            (tue_thu_df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        # Calculate attendance percentage based on average attendance
        avg_eligible = tue_thu_df[
            (tue_thu_df['Division'] == division) & 
            (tue_thu_df['Location'] == 'London UK') & 
            (tue_thu_df['Working Status'] == 'Hybrid') &
            (tue_thu_df['is_full_time'] == True)
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        attendance_percentage = (division_attendance / avg_eligible * 100) if avg_eligible > 0 else 0
        
        result.append({
            'division': division,
            'attendance_count': round(division_attendance, 1),
            'eligible_count': round(avg_eligible, 1),
            'attendance_percentage': round(attendance_percentage, 1)
        })
    
    return pd.DataFrame(result)

def calculate_division_attendance_by_location(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate average daily attendance (#) by division, split into London, Hybrid, Full-Time and Other.
    
    Args:
        df: Combined dataframe with employee and attendance data
        
    Returns:
        DataFrame with division and attendance counts by category
    """
    # Get unique divisions, filtering out NaN values
    unique_divisions = [d for d in df['Division'].unique() if pd.notna(d)]
    unique_divisions.sort()  # Sort alphabetically
    
    result = []
    for division in unique_divisions:
        # Skip divisions with no employees
        if df[df['Division'] == division]['employee_id'].nunique() == 0:
            continue
            
        # Calculate average daily attendance for London, Hybrid, Full-Time
        london_hybrid_ft_count = df[
            (df['Division'] == division) & 
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid') &
            (df['is_full_time'] == True) & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        # Calculate average daily attendance for Hybrid (non-London)
        hybrid_count = df[
            (df['Division'] == division) & 
            (df['Location'] != 'London UK') & 
            (df['Working Status'] == 'Hybrid') &
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        # Calculate average daily attendance for Full-Time (non-Hybrid)
        full_time_count = df[
            (df['Division'] == division) & 
            (df['Working Status'] != 'Hybrid') &
            (df['is_full_time'] == True) & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        # Calculate average daily attendance for Other
        other_count = df[
            (df['Division'] == division) & 
            ~((df['Location'] == 'London UK') & 
              (df['Working Status'] == 'Hybrid') &
              (df['is_full_time'] == True)) &
            ~((df['Location'] != 'London UK') & 
              (df['Working Status'] == 'Hybrid')) &
            ~((df['Working Status'] != 'Hybrid') &
              (df['is_full_time'] == True)) &
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        result.append({
            'division': division,
            'london_hybrid_ft_count': round(london_hybrid_ft_count, 1) if not pd.isna(london_hybrid_ft_count) else 0,
            'hybrid_count': round(hybrid_count, 1) if not pd.isna(hybrid_count) else 0,
            'full_time_count': round(full_time_count, 1) if not pd.isna(full_time_count) else 0,
            'other_count': round(other_count, 1) if not pd.isna(other_count) else 0
        })
    
    return pd.DataFrame(result)

def calculate_period_summary(df: pd.DataFrame, start_date=None, end_date=None) -> pd.DataFrame:
    """Calculate attendance summary by weekday for a given period."""
    df = df.copy()
    
    # Filter for date range only if both dates are provided
    if start_date is not None and end_date is not None:
        date_mask = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
        df = df[date_mask]
    
    # Check if we have full employee info available for consistent calculations
    has_full_employee_info = hasattr(df, 'attrs') and 'full_employee_info' in df.attrs
    
    # Create weekday averages
    weekday_stats = []
    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
        day_mask = (df['day_of_week'] == day)
        
        # London, Hybrid, Full-Time mask
        london_hybrid_ft_mask = (
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid') &
            (df['is_full_time'] == True)
        )
        
        # Get unique dates for this weekday
        day_dates = sorted(df[day_mask]['date_only'].unique())
        
        if not day_dates:
            continue
        
        # Get attendance for this weekday
        london_hybrid_ft_attendance = df[
            day_mask & 
            london_hybrid_ft_mask & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        others_attendance = df[
            day_mask & 
            ~london_hybrid_ft_mask & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        # For Tue, Wed, Thu - use full employee info if available
        if day in ['Tuesday', 'Wednesday', 'Thursday'] and has_full_employee_info:
            # Get full employee info DataFrame
            full_emp_df = df.attrs['full_employee_info']
            
            # Calculate average eligible employees across all dates of this weekday
            total_eligible = 0
            
            for date in day_dates:
                # Apply employment date filter for this date
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
                
                # Count eligible employees for this date
                eligible_count = full_emp_df[
                    active_mask_full & london_hybrid_ft_mask_full
                ]['employee_id'].nunique()
                
                total_eligible += eligible_count
            
            # Calculate average if we found any dates
            eligible_london_hybrid_ft = total_eligible / len(day_dates) if day_dates else 0
        else:
            # For other days or if no lookup available, calculate from current data
            eligible_london_hybrid_ft = df[
                day_mask &
                london_hybrid_ft_mask
            ]['employee_id'].nunique()
        
        # Calculate percentage
        attendance_percentage = (
            (london_hybrid_ft_attendance / eligible_london_hybrid_ft * 100)
            if eligible_london_hybrid_ft > 0 else 0
        )
        
        weekday_stats.append({
            'weekday': day,
            'london_hybrid_ft_count': round(london_hybrid_ft_attendance, 1) if not pd.isna(london_hybrid_ft_attendance) else 0,
            'other_count': round(others_attendance, 1) if not pd.isna(others_attendance) else 0,
            'attendance_percentage': round(attendance_percentage, 1)
        })
    
    return pd.DataFrame(weekday_stats)