import pandas as pd
from .attendance_counts import calculate_mean_arrival_time

def calculate_individual_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate individual employee attendance metrics."""
    result = []
    
    # Ensure Date/time is properly parsed
    df['Date/time'] = pd.to_datetime(df['Date/time'], dayfirst=True)
    df['date_only'] = pd.to_datetime(df['date_only'])
    
    # Add more detailed debugging
    print("\nOverall Dataset Analysis:")
    print(f"Total unique dates: {df['date_only'].nunique()}")
    print(f"Date range: {df['date_only'].min()} to {df['date_only'].max()}")
    print("\nEntrance distribution by hour:")
    print(df['Date/time'].dt.hour.value_counts().sort_index())
    print("\nEntrance distribution by location:")
    print(df['Where'].value_counts())
    
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
        days_attended = emp_data[emp_data['present'] == 'Yes']['date_only'].nunique()
        
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
                (emp_data['present'] == 'Yes') & 
                (emp_data['date_only'].dt.dayofweek.isin([1, 2, 3]))
            ]
            
            # Debug: Print sample of core attendance for this employee
            if not core_attendance.empty:
                print(f"\nDebug - Employee {emp_id} ({emp_name}) core attendance sample:")
                print(f"Date/time dtype: {core_attendance['Date/time'].dtype}")
                print("\nFirst 5 records sorted by Date/time:")
                debug_sample = (
                    core_attendance
                    .sort_values('Date/time')
                    .head()
                    [['date_only', 'Date/time', 'Event', 'Where']]
                )
                print(debug_sample)
            
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
                
                # Flag late entries (after 11:00)
                late_entries = daily_first_entries[daily_first_entries.dt.hour >= 11]
                if not late_entries.empty:
                    print(f"\nLate entries for {emp_name}:")
                    print(late_entries)
                    print(f"Entrance locations for late entries:")
                    print(core_attendance[
                        core_attendance['date_only'].isin(late_entries.index)
                    ]['Where'].value_counts())
                
                # Debug: Print first entries for verification
                print("\nFirst entries of each day:")
                print(daily_first_entries.head())
                
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
                
                # Debug: Print time calculation details
                print(f"\nAverage calculation:")
                print(f"Total minutes: {minutes.tolist()}")
                print(f"Average minutes: {avg_minutes}")
                print(f"Calculated time: {avg_entry_time}")
        
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
    
    # Get unique employees
    unique_employees = df.drop_duplicates('employee_id')[
        ['employee_id', 'Last name, First name']
    ]
    
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
            (emp_data['present'] == 'Yes')
        ]['date_only'].nunique()
        
        # Initialize Tue-Thu metrics
        attended_tue_thu = 0
        employed_tue_thu = 0
        mean_entry_str = None
        median_entry_str = None
        
        # Calculate Tue-Thu metrics for all employees
        tue_thu_mask = emp_data['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])
        attended_tue_thu = emp_data[
            (emp_data['present'] == 'Yes') & 
            tue_thu_mask
        ]['date_only'].nunique()
        
        # Count potential Tue-Thu during employment
        employed_tue_thu = len(employed_dates[employed_dates.dayofweek.isin([1, 2, 3])])
        
        # Calculate attendance rate only for London, Hybrid, Full-Time employees
        attendance_rate = None
        if is_london_hybrid_ft and employed_tue_thu > 0:
            attendance_rate = round(attended_tue_thu / employed_tue_thu * 100, 1)
        
        # Only calculate detailed time metrics for London, Hybrid, Full-Time employees
        if is_london_hybrid_ft:
            # Calculate mean and median entry times for Tue-Thu
            tue_thu_entries = emp_data[
                (emp_data['present'] == 'Yes') & 
                tue_thu_mask
            ].groupby('date_only')['parsed_time'].min()
            
            if not tue_thu_entries.empty:
                # Calculate mean (excluding outliers) and get list of excluded times
                mean_entry_str, excluded_times = calculate_mean_arrival_time(tue_thu_entries)
                
                # Debug logging for excluded times
                if excluded_times:
                    print(f"\nExcluded arrival times for {emp_name}:")
                    for time in excluded_times:
                        print(f"  {time}")
                
                # Calculate median (using all times)
                minutes = pd.Series([
                    t.hour * 60 + t.minute 
                    for t in tue_thu_entries
                ], index=tue_thu_entries.index)
                
                median_minutes = round(minutes.median())
                median_hours = median_minutes // 60
                median_mins = median_minutes % 60
                median_entry_str = f"{int(median_hours):02d}:{int(median_mins):02d}"
        
        results.append({
            'employee_id': emp_id,
            'name': emp_name,
            'is_london_hybrid_ft': is_london_hybrid_ft,
            'total_days_attended': attended_days,
            'tue_thu_days_attended': attended_tue_thu,
            'potential_tue_thu_days': employed_tue_thu,
            'mean_arrival_time': mean_entry_str,
            'median_arrival_time': median_entry_str,
            'attendance_rate': attendance_rate
        })
    
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
        'total_days_attended': 'Total Days Attended',
        'tue_thu_days_attended': 'Tuesday-Thursday Days',
        'potential_tue_thu_days': 'Potential Office Days',
        'mean_arrival_time': 'Mean Arrival Time',
        'median_arrival_time': 'Median Arrival Time',
        'attendance_rate': 'Attendance Rate (%)'
    }
    
    return result_df.rename(columns=column_mapping)