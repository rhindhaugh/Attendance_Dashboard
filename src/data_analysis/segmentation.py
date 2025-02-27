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

def calculate_period_summary(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    """Calculate attendance summary by weekday for a given period."""
    df = df.copy()
    
    # Filter for date range
    date_mask = (df['date_only'] >= start_date) & (df['date_only'] <= end_date)
    df = df[date_mask]
    
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
        
        # Get eligible London, Hybrid, Full-Time employees
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
            'london_hybrid_ft_count': round(london_hybrid_ft_attendance, 1),
            'other_count': round(others_attendance, 1),
            'attendance_percentage': round(attendance_percentage, 1)
        })
    
    return pd.DataFrame(weekday_stats)