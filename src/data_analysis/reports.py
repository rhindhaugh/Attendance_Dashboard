import pandas as pd

def calculate_daily_attendance_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily attendance counts split by employee type."""
    # Calculate attendance for each day
    daily_counts = []
    
    # Check if we have full employee data available for consistent calculations
    has_full_employee_info = hasattr(df, 'attrs') and 'full_employee_info' in df.attrs
    
    for date in sorted(df['date_only'].unique()):
        # Define date_mask for the current date
        date_mask = df['date_only'] == date
        
        # Debug code removed for cleaner output
        
        # FIRST: Calculate present employees directly - this shouldn't change
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
        
        # SECOND: Calculate eligible employees - use full employee info if available, else filtered
        if has_full_employee_info:
            # Get full employee info DataFrame
            full_emp_df = df.attrs['full_employee_info']
            
            # Apply the same conditions to full employee dataset
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
            total_eligible_london_hybrid_ft = full_emp_df[
                active_mask_full & london_hybrid_ft_mask_full
            ]['employee_id'].nunique()
            
            # Compact debugging just for Dec 11, 2024
            if pd.Timestamp(date).strftime('%Y-%m-%d') == '2024-12-11':
                filtered_eligible = df[active_mask & london_hybrid_ft_mask]['employee_id'].nunique()
                print(f"Dec 11, 2024: Full dataset eligible: {total_eligible_london_hybrid_ft}, Filtered: {filtered_eligible}")
        else:
            # Use current filtered dataset if full employee info not available
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