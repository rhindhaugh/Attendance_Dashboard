"""
Common utility functions for data analysis to reduce code duplication.
"""
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import sys
import os

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import LONDON_LOCATION, HYBRID_WORKING_STATUS, CORE_WEEKDAYS, CORE_WEEKDAY_INDICES

logger = logging.getLogger("attendance_dashboard.data_analysis.common")

def get_employment_date_mask(df: pd.DataFrame, date: pd.Timestamp) -> pd.Series:
    """
    Create a mask for employees who were employed on the given date.
    
    Args:
        df: DataFrame with employee data
        date: The date to check employment status for
        
    Returns:
        Boolean mask for employees who were employed on the date
    """
    return (
        (pd.to_datetime(df['Combined hire date']) <= date) & 
        (
            (df['Most recent day worked'].isna()) |  # Still employed
            (pd.to_datetime(df['Most recent day worked']) >= date)  # Not yet left
        )
    )

def get_london_hybrid_ft_mask(df: pd.DataFrame) -> pd.Series:
    """
    Create a mask for London, Hybrid, Full-Time employees.
    
    Args:
        df: DataFrame with employee data
        
    Returns:
        Boolean mask for London, Hybrid, Full-Time employees
    """
    if 'Location' not in df.columns or 'Working Status' not in df.columns:
        logger.warning("Missing required columns for London/Hybrid/FT filtering")
        return pd.Series(False, index=df.index)

    location_mask = (df['Location'] == LONDON_LOCATION)
    working_mask = (df['Working Status'] == HYBRID_WORKING_STATUS)
    
    # Check if is_full_time column exists
    if 'is_full_time' in df.columns:
        full_time_mask = (df['is_full_time'] == True)
    else:
        logger.warning("is_full_time column not found. Assuming all employees are full-time.")
        full_time_mask = pd.Series(True, index=df.index)
        
    return location_mask & working_mask & full_time_mask

def get_core_days_mask(df: pd.DataFrame) -> pd.Series:
    """
    Create a mask for core days (Tuesday-Thursday).
    
    Args:
        df: DataFrame with date information
        
    Returns:
        Boolean mask for core days
    """
    if 'day_of_week' in df.columns:
        return df['day_of_week'].isin(CORE_WEEKDAYS)
    elif 'date_only' in df.columns:
        return df['date_only'].dt.dayofweek.isin(CORE_WEEKDAY_INDICES)
    else:
        logger.warning("No date column found for core days filtering")
        return pd.Series(False, index=df.index)

def calculate_eligible_employees(df: pd.DataFrame, date: pd.Timestamp, 
                               full_employee_df: pd.DataFrame = None) -> int:
    """
    Calculate the number of eligible employees (London, Hybrid, Full-Time) for the given date.
    
    Args:
        df: DataFrame with attendance and employee data
        date: The date to check eligibility for
        full_employee_df: Optional full employee DataFrame for more accurate counts
        
    Returns:
        Number of eligible employees
    """
    # If full employee data is provided, use it for more accurate counts
    if full_employee_df is not None and not full_employee_df.empty:
        # Create employment mask for the full employee dataset
        active_mask = get_employment_date_mask(full_employee_df, date)
        # Create London/Hybrid/FT mask for the full employee dataset
        lhft_mask = get_london_hybrid_ft_mask(full_employee_df)
        # Count eligible employees
        return full_employee_df[active_mask & lhft_mask]['employee_id'].nunique()
    
    # Otherwise, calculate from the filtered dataset
    active_mask = get_employment_date_mask(df, date)
    lhft_mask = get_london_hybrid_ft_mask(df)
    return df[active_mask & lhft_mask]['employee_id'].nunique()

def calculate_present_employees(df: pd.DataFrame, date: pd.Timestamp, 
                              lhft_only: bool = True) -> int:
    """
    Calculate the number of employees present on the given date.
    
    Args:
        df: DataFrame with attendance and employee data
        date: The date to check presence for
        lhft_only: If True, only count London, Hybrid, Full-Time employees
        
    Returns:
        Number of present employees
    """
    # Filter for the specific date
    date_mask = (df['date_only'] == date)
    
    # Filter for employed employees on this date
    active_mask = get_employment_date_mask(df, date)
    
    # Combine with is_present filter
    combined_mask = date_mask & active_mask & (df['is_present'] == True)
    
    # Additionally filter for London/Hybrid/FT if requested
    if lhft_only:
        lhft_mask = get_london_hybrid_ft_mask(df)
        combined_mask = combined_mask & lhft_mask
    
    # Count unique employees
    return df[combined_mask]['employee_id'].nunique()

def get_week_start_date(date: pd.Timestamp) -> pd.Timestamp:
    """
    Get the Monday of the week containing the given date.
    
    Args:
        date: Any date
        
    Returns:
        The Monday of that week
    """
    return date - pd.Timedelta(days=date.dayofweek)

def calculate_attendance_percentage(present: int, eligible: int) -> float:
    """
    Calculate attendance percentage with proper error handling.
    
    Args:
        present: Number of present employees
        eligible: Number of eligible employees
        
    Returns:
        Attendance percentage (0-100)
    """
    if eligible > 0:
        return (present / eligible) * 100
    return 0.0