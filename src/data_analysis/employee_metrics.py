import pandas as pd
import logging
from .attendance_counts import calculate_mean_arrival_time

# Set up logging
logger = logging.getLogger("attendance_dashboard.employee_metrics")

def calculate_individual_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate individual employee attendance metrics."""
    result = []
    
    # Ensure Date/time is properly parsed
    df['Date/time'] = pd.to_datetime(df['Date/time'], dayfirst=True)
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Only print essential dataset summary
    print("\nDataset Summary:")
    print(f"Date range: {df['date_only'].min()} to {df['date_only'].max()}")
    print(f"Total unique dates: {df['date_only'].nunique()}")
    
    # Get the date range of our key card data
    data_start_date = df['date_only'].min()
    data_end_date = df['date_only'].max()
    
    # Get unique employees
    unique_employees = df['employee_id'].unique()
    
    for emp_id in unique_employees:
        emp_data = df[df['employee_id'] == emp_id].copy()
        
        # Get employee info
        first_record = emp_data.iloc[0]
        emp_name = first_record['Last name, First name']
        hire_date = pd.to_datetime(first_record['Combined hire date'])
        last_day = pd.to_datetime(first_record['Most recent day worked'])
        location = first_record['Location']
        working_status = first_record['Working Status']
        status = first_record['Status']
        is_full_time = first_record['is_full_time'] if 'is_full_time' in first_record else False
        
        # If last_day is NaT (for active employees), use the end of our data range
        if pd.isna(last_day) and status == 'Active':
            last_day = data_end_date
        
        # Total days attended (any day)
        days_attended = emp_data[emp_data['is_present'] == True]['date_only'].nunique()
        
        # Initialize core days metrics
        core_days_percentage = None
        avg_entry_time = None
        
        # Only calculate core metrics for London, Hybrid, Full-Time employees
        if location == 'London UK' and working_status == 'Hybrid' and is_full_time:
            # Get all core days (Tue-Thu) during employment AND within our data range
            start_date = max(hire_date, data_start_date)
            end_date = min(last_day, data_end_date)
            
            all_dates = pd.date_range(start=start_date, end=end_date)
            core_weekdays = all_dates[all_dates.dayofweek.isin([1, 2, 3])]  # Tue=1, Wed=2, Thu=3
            total_possible_core_days = len(core_weekdays)
            
            # Filter for attended core days
            core_attendance = emp_data[
                (emp_data['is_present'] == True) & 
                (emp_data['date_only'].dt.dayofweek.isin([1, 2, 3]))
            ]
            
            # Skip printing core attendance sample to reduce terminal output
            
            # Count unique core days attended
            core_days_attended = core_attendance['date_only'].nunique()
            
            # Calculate percentage
            if total_possible_core_days > 0:
                core_days_percentage = round((core_days_attended / total_possible_core_days) * 100, 1)
            
            # Calculate average first entry time for core days
            if not core_attendance.empty:
                # Sort by date/time and get actual first entry of each day
                daily_first_entries = (
                    core_attendance
                    .sort_values('Date/time', ascending=True)
                    .groupby('date_only')['Date/time']
                    .first()
                )
                
                # Flag late entries (after 11:00) but don't print them
                late_entries = daily_first_entries[daily_first_entries.dt.hour >= 11]
                
                # Extract just the time component
                daily_times = daily_first_entries.dt.time
                
                # Convert times to minutes since midnight
                minutes = pd.Series([
                    t.hour * 60 + t.minute 
                    for t in daily_times
                ])
                
                # Calculate average and convert back to time string
                avg_minutes = round(minutes.mean())
                hours = avg_minutes // 60
                mins = avg_minutes % 60
                avg_entry_time = f"{int(hours):02d}:{int(mins):02d}"
        
        result.append({
            'employee_name': emp_name,
            'days_attended': days_attended,
            'core_days_percentage': core_days_percentage,
            'avg_entry_time': avg_entry_time
        })
    
    return pd.DataFrame(result)

def create_employee_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create employee summary table with attendance metrics.
    
    Args:
        df: Combined dataframe with employee and attendance data
        
    Returns:
        DataFrame with employee attendance summary
    """
    # Ensure date columns are datetime
    df['date_only'] = pd.to_datetime(df['date_only'])
    df['Combined hire date'] = pd.to_datetime(df['Combined hire date'])
    df['Most recent day worked'] = pd.to_datetime(df['Most recent day worked'])
    
    # Get the full date range from the data
    date_range = pd.date_range(
        start=df['date_only'].min(),
        end=df['date_only'].max()
    )
    
    # Initialize results list
    results = []
    
    # Get unique employees with relevant columns
    employee_columns = ['employee_id', 'Last name, First name']
    # Add extra columns if they exist in the dataframe
    for col in ['Working Status', 'Location', 'Division']:
        if col in df.columns:
            employee_columns.append(col)
    
    unique_employees = df.drop_duplicates('employee_id')[employee_columns]
    
    for _, emp in unique_employees.iterrows():
        emp_id = emp['employee_id']
        emp_name = emp['Last name, First name']
        
        # Get employee's data
        emp_data = df[df['employee_id'] == emp_id].copy()
        if emp_data.empty:
            continue
            
        # Get first record for this employee
        first_record = emp_data.iloc[0]
        
        # Get employment dates
        hire_date = pd.to_datetime(first_record['Combined hire date'])
        last_day = pd.to_datetime(first_record['Most recent day worked'])
        
        # Check if employee is London, Hybrid, Full-Time
        is_london_hybrid_ft = (
            first_record['Location'] == 'London UK' and
            first_record['Working Status'] == 'Hybrid' and
            first_record['is_full_time'] == True
        )
        
        # If last_day is NaT (active employee), use the end of our data range
        if pd.isna(last_day):
            last_day = date_range[-1]
            
        # Filter for days when employee was employed
        employed_dates = date_range[
            (date_range >= hire_date) & 
            (date_range <= last_day)
        ]
        
        # Get attended days
        attended_days = emp_data[
            (emp_data['is_present'] == True)
        ]['date_only'].nunique()
        
        # Initialize Tue-Thu metrics
        attended_tue_thu = 0
        employed_tue_thu = 0
        mean_entry_str = None
        median_entry_str = None
        
        # Calculate Tue-Thu metrics for all employees
        tue_thu_mask = emp_data['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])
        attended_tue_thu = emp_data[
            (emp_data['is_present'] == True) & 
            tue_thu_mask
        ]['date_only'].nunique()
        
        # Count potential Tue-Thu during employment
        employed_tue_thu = len(employed_dates[employed_dates.dayofweek.isin([1, 2, 3])])
        
        # Calculate attendance rate only for London, Hybrid, Full-Time employees
        attendance_rate = None
        if is_london_hybrid_ft and employed_tue_thu > 0:
            attendance_rate = round(attended_tue_thu / employed_tue_thu * 100, 1)
        
        # Calculate arrival time metrics for ALL employees who have at least one entry
        # First, get all entries for this employee
        all_entries = emp_data[emp_data['is_present'] == True]
        
        # Calculate metrics if we have any entries
        mean_entry_str = None
        mean_no_outliers_str = None
        median_entry_str = None
        
        if not all_entries.empty:
            # Get first entry of each day
            daily_entries = all_entries.groupby('date_only')['parsed_time'].min()
            
            if not daily_entries.empty and not daily_entries.isna().all():
                # Calculate median time (using all times)
                minutes = pd.Series([
                    t.hour * 60 + t.minute 
                    for t in daily_entries if pd.notna(t)
                ], index=[idx for idx, t in zip(daily_entries.index, daily_entries) if pd.notna(t)])
                
                if not minutes.empty:
                    # Calculate true mean (including all values)
                    mean_minutes = round(minutes.mean())
                    mean_hours = mean_minutes // 60
                    mean_mins = mean_minutes % 60
                    mean_entry_str = f"{int(mean_hours):02d}:{int(mean_mins):02d}"
                    
                    # Calculate median
                    median_minutes = round(minutes.median())
                    median_hours = median_minutes // 60
                    median_mins = median_minutes % 60
                    median_entry_str = f"{int(median_hours):02d}:{int(median_mins):02d}"
                    
                    # Calculate mean excluding outliers and get list of excluded times
                    mean_no_outliers_str, excluded_times = calculate_mean_arrival_time(daily_entries)
                    
                    # We'll skip printing the excluded arrival times to keep terminal output shorter
        
        # Create result dictionary with basic fields
        result_dict = {
            'employee_id': emp_id,
            'name': emp_name,
            'is_london_hybrid_ft': is_london_hybrid_ft,
            'total_days_attended': attended_days,
            'tue_thu_days_attended': attended_tue_thu,
            'potential_tue_thu_days': employed_tue_thu,
            'mean_arrival_time': mean_entry_str,  # All values included
            'mean_arrival_no_outliers': mean_no_outliers_str,  # Outliers excluded
            'median_arrival_time': median_entry_str,
            'attendance_rate': attendance_rate
        }
        
        # Add Working Status, Location, Division, and Full Time status if they exist
        for col in ['Working Status', 'Location', 'Division']:
            if col in emp and pd.notna(emp[col]):
                result_dict[col.lower().replace(' ', '_')] = emp[col]
                
        # Add is_full_time status from the first record (already set in is_london_hybrid_ft check)
        result_dict['is_full_time'] = first_record['is_full_time'] if 'is_full_time' in first_record else False
        
        results.append(result_dict)
    
    # Convert to DataFrame and sort by London, Hybrid, Full-Time first, then attendance rate
    result_df = pd.DataFrame(results)
    
    # Sort by London, Hybrid, Full-Time and then by attendance rate
    result_df = result_df.sort_values(
        ['is_london_hybrid_ft', 'attendance_rate'], 
        ascending=[False, False]
    )
    
    # Round numeric columns
    numeric_cols = ['total_days_attended', 'tue_thu_days_attended', 'potential_tue_thu_days']
    for col in numeric_cols:
        if col in result_df.columns:
            result_df[col] = result_df[col].round(1)
    
    # Format attendance rate as percentage
    result_df['attendance_rate'] = result_df['attendance_rate'].apply(
        lambda x: f"{x:.1f}%" if pd.notnull(x) else None
    )
    
    # Remove the is_london_hybrid_ft column from the final output
    result_df = result_df.drop(columns=['is_london_hybrid_ft'])
    
    # Rename columns to be more readable
    column_mapping = {
        'employee_id': 'Employee ID',
        'name': 'Employee Name',
        'working_status': 'Working Status',
        'location': 'Location',
        'division': 'Division',
        'is_full_time': 'Full-Time',
        'total_days_attended': 'Total Days Attended',
        'tue_thu_days_attended': 'Tuesday-Thursday Days',
        'potential_tue_thu_days': 'Potential Office Days',
        'mean_arrival_time': 'Mean Arrival Time (All)',
        'mean_arrival_no_outliers': 'Mean Arrival Time (No Outliers)',
        'median_arrival_time': 'Median Arrival Time',
        'attendance_rate': 'Attendance Rate (%)'
    }
    
    return result_df.rename(columns=column_mapping)

def get_daily_employee_attendance(df: pd.DataFrame, selected_date: pd.Timestamp) -> pd.DataFrame:
    """
    Get attendance data for all active London Hybrid Full-Time employees on a specific date.
    
    Args:
        df: Combined dataframe with employee and attendance data
        selected_date: The specific date to check attendance
        
    Returns:
        DataFrame with London Hybrid Full-Time employee attendance and arrival times for the selected date
    """
    # Ensure df is a copy to avoid modifying the original
    df = df.copy()
    
    # Ensure employee_id is numeric for consistent comparisons
    if 'employee_id' in df.columns:
        df['employee_id'] = pd.to_numeric(df['employee_id'], errors='coerce')
    # Ensure selected_date is a pandas Timestamp
    if not isinstance(selected_date, pd.Timestamp):
        selected_date = pd.to_datetime(selected_date)
    
    # Get all LONDON HYBRID FULL-TIME employees who were active on the selected date
    # Active = hired before or on selected date, and either still active or left after selected date
    # Ensure ALL datetime and ID comparisons are safe by explicit conversion
    
    # Make sure selected_date is properly formatted
    if not isinstance(selected_date, pd.Timestamp):
        selected_date = pd.to_datetime(selected_date)
    
    # Convert date columns to datetime to ensure safe comparisons
    # Use .copy() to prevent SettingWithCopyWarning
    hire_dates = pd.to_datetime(df['Combined hire date'], errors='coerce')
    last_days = df['Most recent day worked'].copy()
    # Convert non-NaT values to datetime
    mask = ~last_days.isna()
    last_days.loc[mask] = pd.to_datetime(last_days.loc[mask], errors='coerce')
    
    # Start with date comparisons - ensure all types match for comparison
    active_london_hybrid_ft_mask = (
        (hire_dates <= selected_date) &
        (
            last_days.isna() |
            (last_days >= selected_date)
        ) &
        (df['Location'].astype(str) == 'London UK') &  # London employees
        (df['Working Status'].astype(str) == 'Hybrid')  # Hybrid working status
    )
    
    # Add is_full_time check only if the column exists
    if 'is_full_time' in df.columns:
        active_london_hybrid_ft_mask = active_london_hybrid_ft_mask & (df['is_full_time'] == True)
    else:
        # If is_full_time column doesn't exist, assume all employees are full-time
        print("Warning: is_full_time column not found. Assuming all employees are full-time.")
    
    # Get unique active London Hybrid Full-Time employees
    # Get a copy of the filtered data to avoid SettingWithCopyWarning
    active_employees = df[active_london_hybrid_ft_mask].drop_duplicates('employee_id')[
        ['employee_id', 'Last name, First name', 'Working Status', 'Location', 'Division', 'Department']
    ].copy()
    
    # Ensure employee_id is numeric in active_employees using .loc accessor - explicitly convert to float64
    active_employees.loc[:, 'employee_id'] = pd.to_numeric(active_employees['employee_id'], errors='coerce').astype('float64')
    
    # Filter data for the selected date - ensure date formats match exactly
    # Convert date_only to the same datetime format as selected_date for safe comparison
    date_only_dt = pd.to_datetime(df['date_only'])
    date_mask = date_only_dt == selected_date
    
    # Create an explicit copy to avoid SettingWithCopyWarning
    date_data = df[date_mask].copy()
    
    # Ensure employee_id is numeric in date_data too - use proper .loc accessor and explicitly convert to float64
    date_data.loc[:, 'employee_id'] = pd.to_numeric(date_data['employee_id'], errors='coerce').astype('float64')
    
    # For each active London employee, check if they attended on the selected date
    attendance_data = []
    
    # Handle case where no employees match the criteria
    if active_employees.empty:
        return pd.DataFrame()  # Return empty DataFrame
        
    for _, employee in active_employees.iterrows():
        emp_id = employee['employee_id']
        
        # Double-check that we're comparing the same types
        # Both should be float64 at this point, but let's be extra careful
        try:
            # Use an equality mask instead of direct DataFrame filtering
            # This avoids any potential issues with index types
            emp_id_mask = date_data['employee_id'].astype('float64') == float(emp_id)
            emp_date_data = date_data[emp_id_mask]
            attended = not emp_date_data.empty
        except (ValueError, TypeError):
            # Fallback if there's any issue with type conversion
            logger.warning(f"Type conversion issue with employee_id: {emp_id}, type: {type(emp_id)}")
            # Try string comparison as absolute last resort
            emp_id_str = str(emp_id).strip()
            date_id_str = date_data['employee_id'].astype(str).str.strip()
            emp_date_data = date_data[date_id_str == emp_id_str]
            attended = not emp_date_data.empty
        
        # Get arrival time if attended
        arrival_time = None
        if attended:
            try:
                # Get the earliest scan for this employee on this date
                earliest_scan = emp_date_data.sort_values('parsed_time').iloc[0]
                
                # Check if parsed_time is valid before formatting
                if pd.notna(earliest_scan['parsed_time']):
                    arrival_time = earliest_scan['parsed_time'].strftime('%H:%M')
                else:
                    arrival_time = "Unknown"
            except Exception as e:
                # Fallback if there's any error getting the arrival time
                arrival_time = "Error"
        
        # Create attendance record
        attendance_record = {
            'employee_id': emp_id,
            'Employee Name': employee['Last name, First name'],
            'Working Status': employee['Working Status'],
            'Location': employee['Location'],
            'Division': employee['Division'],
            'Department': employee['Department'],
            'Attended': 'Yes' if attended else 'No',
            'Arrival Time': arrival_time if attended else 'N/A'
        }
        
        attendance_data.append(attendance_record)
    
    # Convert to DataFrame
    result_df = pd.DataFrame(attendance_data)
    
    # Sort by attendance status (Yes first), then by Employee Name
    if not result_df.empty:
        result_df = result_df.sort_values(['Attended', 'Employee Name'], ascending=[False, True])
    
    return result_df