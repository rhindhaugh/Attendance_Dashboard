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
    
    # Check if 'present' column exists
    if 'present' not in tue_thu_df.columns:
        # If 'present' column doesn't exist, we assume everyone in the dataset was present
        tue_thu_df['present'] = 'Yes'
    
    # Check if 'is_full_time' exists in the dataset
    has_full_time = 'is_full_time' in tue_thu_df.columns
    
    result = []
    for division in unique_divisions:
        # Base filter for this division
        division_filter = (tue_thu_df['Division'] == division)
        
        # Get London, Hybrid employees in this division
        location_filter = (tue_thu_df['Location'] == 'London UK')
        working_filter = (tue_thu_df['Working Status'] == 'Hybrid')
        
        # Add full-time filter if the column exists, otherwise assume all are full-time
        if has_full_time:
            full_time_filter = (tue_thu_df['is_full_time'] == True)
            combined_filter = division_filter & location_filter & working_filter & full_time_filter
        else:
            combined_filter = division_filter & location_filter & working_filter
        
        # Get all unique eligible employees
        division_employees = tue_thu_df[combined_filter]['employee_id'].nunique()
        
        # Skip divisions with no eligible employees
        if division_employees == 0:
            continue
        
        # Get present employees for each day
        present_filter = combined_filter & (tue_thu_df['present'] == 'Yes')
        daily_attendance = tue_thu_df[present_filter].groupby('date_only')['employee_id'].nunique()
        
        # Get eligible employees for each day
        daily_eligible = tue_thu_df[combined_filter].groupby('date_only')['employee_id'].nunique()
        
        # Calculate daily percentages then average them
        # This handles days with no eligible employees properly
        daily_percentages = []
        attendance_count = 0
        eligible_count = 0
        
        # Combine the two series to ensure we have all dates
        all_dates = sorted(set(daily_attendance.index) | set(daily_eligible.index))
        
        for date in all_dates:
            # Get attendance and eligible counts for this date
            attendance = daily_attendance.get(date, 0)
            eligible = daily_eligible.get(date, 0)
            
            # Only calculate percentage if there are eligible employees
            if eligible > 0:
                daily_percentages.append((attendance / eligible) * 100)
                attendance_count += attendance
                eligible_count += eligible
        
        # Calculate final values
        if len(daily_percentages) > 0:
            # Average of daily percentages
            avg_daily_percentage = sum(daily_percentages) / len(daily_percentages)
            
            # Average counts over all days
            avg_attendance = attendance_count / len(all_dates)
            avg_eligible = eligible_count / len(all_dates)
            
            result.append({
                'division': division,
                'attendance_count': round(avg_attendance, 1),
                'eligible_count': round(avg_eligible, 1),
                'attendance_percentage': round(avg_daily_percentage, 1)
            })
        else:
            # No valid days with eligible employees
            result.append({
                'division': division,
                'attendance_count': 0,
                'eligible_count': 0,
                'attendance_percentage': 0
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
    
    # Check if 'present' column exists
    if 'present' not in df.columns:
        # If 'present' column doesn't exist, we assume everyone in the dataset was present
        df['present'] = 'Yes'
    
    # Check if 'is_full_time' exists in the dataset
    has_full_time = 'is_full_time' in df.columns
    
    result = []
    for division in unique_divisions:
        # Skip divisions with no employees
        if df[df['Division'] == division]['employee_id'].nunique() == 0:
            continue
        
        # Base division filter
        division_filter = (df['Division'] == division) & (df['present'] == 'Yes')
        
        # London, Hybrid, Full-Time filter
        if has_full_time:
            london_hybrid_ft_filter = (
                division_filter & 
                (df['Location'] == 'London UK') & 
                (df['Working Status'] == 'Hybrid') &
                (df['is_full_time'] == True)
            )
        else:
            # If no is_full_time column, just use Location and Working Status
            london_hybrid_ft_filter = (
                division_filter & 
                (df['Location'] == 'London UK') & 
                (df['Working Status'] == 'Hybrid')
            )
        
        # Calculate average daily attendance for London, Hybrid, Full-Time
        london_hybrid_ft_df = df[london_hybrid_ft_filter]
        london_hybrid_ft_daily = london_hybrid_ft_df.groupby('date_only')['employee_id'].nunique()
        london_hybrid_ft_count = london_hybrid_ft_daily.mean() if not london_hybrid_ft_daily.empty else 0
        
        # Hybrid (non-London) filter
        hybrid_filter = (
            division_filter & 
            (df['Location'] != 'London UK') & 
            (df['Working Status'] == 'Hybrid')
        )
        
        # Calculate average daily attendance for Hybrid (non-London)
        hybrid_df = df[hybrid_filter]
        hybrid_daily = hybrid_df.groupby('date_only')['employee_id'].nunique()
        hybrid_count = hybrid_daily.mean() if not hybrid_daily.empty else 0
        
        # Full-Time (non-Hybrid) filter
        if has_full_time:
            full_time_filter = (
                division_filter & 
                (df['Working Status'] != 'Hybrid') &
                (df['is_full_time'] == True)
            )
        else:
            # If no is_full_time column, just use Working Status
            full_time_filter = (
                division_filter & 
                (df['Working Status'] != 'Hybrid')
            )
        
        # Calculate average daily attendance for Full-Time (non-Hybrid)
        full_time_df = df[full_time_filter]
        full_time_daily = full_time_df.groupby('date_only')['employee_id'].nunique()
        full_time_count = full_time_daily.mean() if not full_time_daily.empty else 0
        
        # Other filter (everyone else present)
        other_filter = (
            division_filter & 
            ~london_hybrid_ft_filter &
            ~hybrid_filter &
            ~full_time_filter
        )
        
        # Calculate average daily attendance for Other
        other_df = df[other_filter]
        other_daily = other_df.groupby('date_only')['employee_id'].nunique()
        other_count = other_daily.mean() if not other_daily.empty else 0
        
        result.append({
            'division': division,
            'london_hybrid_ft_count': round(london_hybrid_ft_count, 1),
            'hybrid_count': round(hybrid_count, 1),
            'full_time_count': round(full_time_count, 1),
            'other_count': round(other_count, 1)
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