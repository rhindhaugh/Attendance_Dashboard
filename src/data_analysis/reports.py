import pandas as pd
import logging
import sys
import os

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.data_analysis.common import (
    get_employment_date_mask,
    get_london_hybrid_ft_mask,
    get_core_days_mask,
    calculate_eligible_employees,
    calculate_present_employees,
    calculate_attendance_percentage,
    get_week_start_date
)
from src.utils import handle_empty_dataframe, validate_columns

logger = logging.getLogger("attendance_dashboard.data_analysis.reports")

def calculate_daily_attendance_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily attendance counts split by employee type.
    
    This function provides daily attendance metrics split into two categories:
    1. London, Hybrid, Full-Time employees
    2. All other employees
    
    Args:
        df: DataFrame with attendance and employee data
        
    Returns:
        DataFrame with daily attendance counts and percentages
    """
    # Validate input
    if handle_empty_dataframe(df, "calculate_daily_attendance_counts", logger):
        return pd.DataFrame()
        
    required_columns = ['date_only', 'employee_id', 'is_present', 'Combined hire date']
    if not validate_columns(df, required_columns, "calculate_daily_attendance_counts", logger):
        return pd.DataFrame()
    
    # Get full employee DataFrame if available
    full_employee_df = df.attrs['full_employee_info'] if hasattr(df, 'attrs') and 'full_employee_info' in df.attrs else None
    
    # Initialize results list
    daily_counts = []
    logger.debug(f"Calculating daily attendance counts for {df['date_only'].nunique()} unique dates")
    
    # Process each date
    for date in sorted(df['date_only'].unique()):
        try:
            # Calculate London, Hybrid, Full-Time employees who were present
            lhft_present = calculate_present_employees(df, date, lhft_only=True)
            
            # Calculate other employees who were present
            # We need a specialized approach since common function only handles LHFT
            active_mask = get_employment_date_mask(df, date)
            lhft_mask = get_london_hybrid_ft_mask(df)
            date_mask = (df['date_only'] == date)
            others_present = df[
                date_mask & active_mask & ~lhft_mask & (df['is_present'] == True)
            ]['employee_id'].nunique()
            
            # Calculate total eligible London, Hybrid, Full-Time employees
            eligible_lhft = calculate_eligible_employees(df, date, full_employee_df)
            
            # Calculate attendance percentage
            attendance_percentage = calculate_attendance_percentage(lhft_present, eligible_lhft)
            
            # Special debugging for Dec 11, 2024 (if present in the data)
            if pd.Timestamp(date).strftime('%Y-%m-%d') == '2024-12-11' and full_employee_df is not None:
                filtered_eligible = df[active_mask & lhft_mask]['employee_id'].nunique()
                logger.debug(f"Dec 11, 2024: Full dataset eligible: {eligible_lhft}, Filtered: {filtered_eligible}")
            
            # Add results to daily counts
            daily_counts.append({
                'date': date,
                'day_of_week': pd.Timestamp(date).strftime('%A'),
                'london_hybrid_ft_count': lhft_present,
                'other_count': others_present,
                'eligible_london_hybrid_ft': eligible_lhft,
                'london_hybrid_ft_percentage': round(attendance_percentage, 1),
                'total_attendance': lhft_present + others_present
            })
            
        except Exception as e:
            logger.error(f"Error calculating attendance counts for date {date}: {e}")
            # Skip this date but continue processing others
    
    logger.info(f"Completed daily attendance count calculation for {len(daily_counts)} dates")
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