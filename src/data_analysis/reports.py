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
            date_mask & active_mask & london_hybrid_ft_mask & (df['is_present'] == True)
        ]['employee_id'].nunique()
        
        # Count others who were present
        others_count = df[
            date_mask & active_mask & ~london_hybrid_ft_mask & (df['is_present'] == True)
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
    FIXED: Now uses the same calculation logic as daily attendance for consistent results.
    """
    df = df.copy()
    
    # Add week start date (Monday)
    df['week_start'] = df['date_only'] - pd.to_timedelta(df['date_only'].dt.dayofweek, unit='d')
    
    # Check if we have full employee info available for consistent calculations
    has_full_employee_info = hasattr(df, 'attrs') and 'full_employee_info' in df.attrs
    
    weekly_counts = []
    for week_start in sorted(df['week_start'].unique()):
        week_end = week_start + pd.Timedelta(days=6)
        
        # Get daily eligible counts and attendance counts for Tue-Thu
        daily_eligible_lhft = []  # London Hybrid Full-Time eligible
        daily_lhft_present = []   # London Hybrid Full-Time present
        daily_others_present = [] # Others present
        
        for date in pd.date_range(week_start, week_end):
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
                
                # Get eligible employee count - using full employee info if available
                if has_full_employee_info:
                    # Get full employee info DataFrame
                    full_emp_df = df.attrs['full_employee_info']
                    
                    # Apply employment date filter to full dataset
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
                    eligible_count = full_emp_df[
                        active_mask_full & london_hybrid_ft_mask_full
                    ]['employee_id'].nunique()
                else:
                    # Use current filtered dataset if full employee info not available
                    eligible_count = df[
                        active_mask & london_hybrid_ft_mask
                    ]['employee_id'].nunique()
                
                # Count LHFT present on this specific date
                date_mask = (df['date_only'] == date)
                lhft_present = df[
                    date_mask & 
                    active_mask &
                    london_hybrid_ft_mask & 
                    (df['is_present'] == True)
                ]['employee_id'].nunique()
                
                # Count others present on this specific date
                others_present = df[
                    date_mask & 
                    active_mask &
                    ~london_hybrid_ft_mask & 
                    (df['is_present'] == True)
                ]['employee_id'].nunique()
                
                # Add to daily counts
                daily_eligible_lhft.append(eligible_count)
                daily_lhft_present.append(lhft_present)
                daily_others_present.append(others_present)
        
        # Skip if no data for this week
        if not daily_eligible_lhft:
            continue
        
        # Calculate averages
        avg_eligible_lhft = sum(daily_eligible_lhft) / len(daily_eligible_lhft) if daily_eligible_lhft else 0
        avg_lhft_present = sum(daily_lhft_present) / len(daily_lhft_present) if daily_lhft_present else 0  
        avg_others_present = sum(daily_others_present) / len(daily_others_present) if daily_others_present else 0
        
        # Calculate percentage
        attendance_percentage = (avg_lhft_present / avg_eligible_lhft * 100) if avg_eligible_lhft > 0 else 0
        
        weekly_counts.append({
            'week_start': week_start,
            'london_hybrid_ft_avg': round(avg_lhft_present, 1),
            'other_avg': round(avg_others_present, 1),
            'avg_eligible_london_hybrid_ft': round(avg_eligible_lhft, 1),
            'london_hybrid_ft_percentage': round(attendance_percentage, 1),
            'total_avg_attendance': round(avg_lhft_present + avg_others_present, 1)
        })
    
    return pd.DataFrame(weekly_counts)