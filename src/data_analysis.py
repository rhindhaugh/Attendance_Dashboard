import pandas as pd

def build_attendance_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build attendance table from key card data.
    """
    df = df.copy()
    
    # Debug step 1: Show earliest 3 scans per day per employee
    print("\n[DEBUG] Earliest 3 scans per day per employee:")
    temp = (
        df
        .sort_values(["employee_id", "date_only", "parsed_time"])
        .groupby(["employee_id", "date_only"])
        .head(3)
    )
    print(temp[["employee_id", "date_only", "parsed_time", "Where"]].head(50))
    
    # Debug step 3: Check location/working status filtering
    location_mask = (df["Location"] == "London UK")
    working_mask = (df["Working Status"] == "Hybrid")
    print("\n[DEBUG] Location/Working Status Analysis:")
    print(f"Total rows: {len(df)}")
    print(f"Rows with non-London or non-Hybrid: {len(df[~(location_mask & working_mask)])}")
    
    # Group value counts for investigation
    print("\nLocation distribution:")
    print(df["Location"].value_counts())
    print("\nWorking Status distribution:")
    print(df["Working Status"].value_counts())
    
    # 1) Get unique employees and dates
    unique_employees = df[["employee_id", "Last name, First name"]].drop_duplicates()
    unique_dates = df["date_only"].unique()
    
    # 2) Build cartesian product (every employee x every date)
    employee_dates = []
    for _, employee in unique_employees.iterrows():
        for date in unique_dates:
            employee_dates.append({
                "employee_id": employee["employee_id"],
                "employee_name": employee["Last name, First name"],
                "date_only": date
            })
    
    cross_df = pd.DataFrame(employee_dates)
    
    # 3) Mark presence by checking if there's data for that employee-date
    attendance = (
        df.groupby(["employee_id", "date_only"])
        .size()
        .reset_index(name="visits")
    )
    
    # Merge attendance data with our cartesian product
    merged = cross_df.merge(
        attendance,
        on=["employee_id", "date_only"],
        how="left"
    )
    
    # After marking presence
    merged["visits"] = merged["visits"].fillna(0)
    merged["present"] = merged["visits"].map({0: "No"}).fillna("Yes")
    
    # Debug step 2: Show rows marked as 'present'
    print("\n[DEBUG] Checking presence logic. Sample 'present' rows:")
    present_only = merged[merged["present"] == "Yes"]
    print(present_only[["employee_id", "employee_name", "date_only", "present", "visits"]].head(30))
    
    # 4) Calculate total days attended per employee
    days_attended = (
        merged[merged["present"] == "Yes"]
        .groupby("employee_id")
        .size()
        .reset_index(name="days_attended")
    )
    
    # Merge days_attended back to our main table
    final_df = merged.merge(days_attended, on="employee_id", how="left")
    
    # Sort by employee name and date
    final_df = final_df.sort_values(["employee_name", "date_only"])
    
    return final_df

def calculate_visit_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count the number of visits (rows in the key card data) per employee_id.
    """
    return (
        df.groupby("employee_id")
        .size()
        .reset_index(name="visit_count")
    )

def calculate_average_arrival_hour(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the average arrival hour for each employee."""
    df = df.copy()
    
    # Get first scan of each day for each employee
    first_scans = (
        df.sort_values(['employee_id', 'date_only', 'parsed_time'])
        .groupby(['employee_id', 'date_only'])
        .first()
        .reset_index()
    )
    
    # Calculate arrival hour from parsed_time
    first_scans['arrival_hour'] = first_scans['parsed_time'].dt.hour
    
    # Calculate average arrival hour per employee
    return (
        first_scans
        .groupby('employee_id')['arrival_hour']
        .mean()
        .round(2)
        .reset_index()
    )

def calculate_daily_attendance_percentage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the daily attendance percentage for hybrid employees.
    Only counts employees who:
    - Are based in London UK
    - Have hybrid working status
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
        
        # Filter for London & Hybrid employees
        location_mask = (df['Location'] == 'London UK')
        working_mask = (df['Working Status'] == 'Hybrid')
        
        # Get total eligible employees for this date
        eligible_employees = df[
            active_mask & location_mask & working_mask
        ]['employee_id'].nunique()
        
        # Get employees who were present
        present_employees = df[
            (df['date_only'] == date) &
            active_mask & 
            location_mask & 
            working_mask &
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
        
        # Get eligible employees for this week
        eligible_employees = df[
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid') &
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
            'london_hybrid_count': sum((x['Location'] == 'London UK') & 
                                     (x['Working Status'] == 'Hybrid') &
                                     (x['present'] == 'Yes')),
            'other_count': sum(~((x['Location'] == 'London UK') & 
                               (x['Working Status'] == 'Hybrid')) &
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
        # Get division employees
        division_employees = df[
            (df['Division'] == division) & 
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid')
        ].drop_duplicates('employee_id')
        
        if len(division_employees) == 0:
            continue
        
        # Count unique employee-days of attendance
        attendance_days = df[
            (df['Division'] == division) & 
            (df['Location'] == 'London UK') & 
            (df['Working Status'] == 'Hybrid') & 
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
        
        # If last_day is NaT (for active employees), use the end of our data range
        if pd.isna(last_day) and status == 'Active':
            last_day = data_end_date
        
        # Total days attended (any day)
        days_attended = emp_data[emp_data['present'] == 'Yes']['date_only'].nunique()
        
        # Initialize core days metrics
        core_days_percentage = None
        avg_entry_time = None
        
        # Only calculate core metrics for London Hybrid employees
        if location == 'London UK' and working_status == 'Hybrid':
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

def calculate_mean_arrival_time(times_series: pd.Series) -> tuple[str, list]:
    """
    Calculate mean arrival time while excluding outliers.
    
    Args:
        times_series: Series of datetime.time objects
        
    Returns:
        tuple: (formatted_mean_time, list_of_excluded_times)
    """
    if times_series.empty:
        return None, []
        
    # Convert times to minutes since midnight
    minutes = pd.Series([
        t.hour * 60 + t.minute 
        for t in times_series
    ], index=times_series.index)
    
    # Calculate median
    median_minutes = minutes.median()
    
    # Define outlier threshold (2 hours = 120 minutes)
    threshold = 120
    
    # Identify and exclude outliers
    is_outlier = abs(minutes - median_minutes) > threshold
    clean_minutes = minutes[~is_outlier]
    excluded_times = times_series[is_outlier]
    
    if clean_minutes.empty:
        return None, list(excluded_times)
    
    # Calculate mean of non-outlier times
    mean_minutes = round(clean_minutes.mean())
    mean_hours = mean_minutes // 60
    mean_mins = mean_minutes % 60
    
    return f"{int(mean_hours):02d}:{int(mean_mins):02d}", list(excluded_times)

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
        
        # Get Tue-Thu metrics
        tue_thu_mask = emp_data['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])
        attended_tue_thu = emp_data[
            (emp_data['present'] == 'Yes') & 
            tue_thu_mask
        ]['date_only'].nunique()
        
        # Count potential Tue-Thu during employment
        employed_tue_thu = len(employed_dates[employed_dates.dayofweek.isin([1, 2, 3])])
        
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
        else:
            mean_entry_str = None
            median_entry_str = None
        
        results.append({
            'employee_id': emp_id,
            'name': emp_name,
            'total_days_attended': attended_days,
            'tue_thu_days_attended': attended_tue_thu,
            'potential_tue_thu_days': employed_tue_thu,
            'mean_arrival_time': mean_entry_str,
            'median_arrival_time': median_entry_str,
            'attendance_rate': round(attended_tue_thu / employed_tue_thu * 100, 1) if employed_tue_thu > 0 else 0
        })
    
    # Convert to DataFrame and sort by attendance rate
    result_df = pd.DataFrame(results).sort_values('attendance_rate', ascending=False)
    
    # Round numeric columns
    numeric_cols = ['total_days_attended', 'tue_thu_days_attended', 'potential_tue_thu_days', 'attendance_rate']
    result_df[numeric_cols] = result_df[numeric_cols].round(1)
    
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
    
    # Format percentage columns
    result_df['attendance_rate'] = result_df['attendance_rate'].apply(
        lambda x: f"{x:.1f}%" if pd.notnull(x) else None
    )
    
    return result_df.rename(columns=column_mapping)

def calculate_tue_thu_attendance_percentage(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily attendance percentage, excluding Mon/Fri."""
    df = df.copy()
    
    # Filter for Tue-Thu
    tue_thu_mask = df['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])
    df = df[tue_thu_mask]
    
    # Get all unique dates and sort them
    all_dates = sorted(df['date_only'].unique())
    
    daily_attendance = []
    for date in all_dates:
        # Filter for employees who were employed on this date
        active_mask = (
            (pd.to_datetime(df['Combined hire date']) <= date) &
            (
                (df['Most recent day worked'].isna()) |
                (pd.to_datetime(df['Most recent day worked']) >= date)
            )
        )
        
        # Filter for London & Hybrid employees
        london_hybrid_mask = (df['Location'] == 'London UK') & (df['Working Status'] == 'Hybrid')
        
        # Calculate metrics
        eligible_employees = df[active_mask & london_hybrid_mask]['employee_id'].nunique()
        present_employees = df[
            (df['date_only'] == date) &
            active_mask & 
            london_hybrid_mask &
            (df['present'] == 'Yes')
        ]['employee_id'].nunique()
        
        # Calculate percentage
        percentage = (present_employees / eligible_employees * 100) if eligible_employees > 0 else 0
        
        daily_attendance.append({
            'date': date,
            'total_eligible': eligible_employees,
            'total_present': present_employees,
            'percentage': round(percentage, 1)
        })
    
    return pd.DataFrame(daily_attendance)

def calculate_daily_attendance_counts(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate daily attendance counts split by employee type."""
    # Calculate attendance for each day
    daily_counts = []
    
    for date in sorted(df['date_only'].unique()):
        # Define date_mask first so it's available for both debug and regular code
        date_mask = df['date_only'] == date
        
        # Debug code for September 12, 2024
        if pd.Timestamp(date).strftime('%d/%m/%Y') == '12/09/2024':
            print("\n===== DEBUGGING SEPTEMBER 12, 2024 =====")
            
            # Get all employees present that day regardless of other criteria
            all_present = df[date_mask & (df['present'] == 'Yes')]['employee_id'].unique()
            print(f"Total unique employees present on Sep 12: {len(all_present)}")
            
            # Check employment dates
            active_mask = (
                (pd.to_datetime(df['Combined hire date']) <= date) & 
                ((df['Most recent day worked'].isna()) | (pd.to_datetime(df['Most recent day worked']) >= date))
            )
            
            active_present = df[date_mask & active_mask & (df['present'] == 'Yes')]['employee_id'].unique()
            print(f"Employees present and active based on hire/departure dates: {len(active_present)}")
            
            # How many were excluded due to employment dates?
            employment_excluded = set(all_present) - set(active_present)
            print(f"Excluded due to employment dates: {len(employment_excluded)}")
            
            # Show details of excluded employees
            if employment_excluded:
                print("\nEmployees excluded due to employment dates:")
                for emp_id in employment_excluded:
                    emp_data = df[(df['employee_id'] == emp_id) & date_mask]
                    if not emp_data.empty:
                        row = emp_data.iloc[0]
                        name = row['Last name, First name'] if 'Last name, First name' in row else 'Unknown'
                        hire_date = row['Combined hire date'] if 'Combined hire date' in row else 'Unknown'
                        last_day = row['Most recent day worked'] if 'Most recent day worked' in row else 'Unknown'
                        print(f"  Employee ID: {emp_id}")
                        print(f"  Name: {name}")
                        print(f"  Hire date: {hire_date}")
                        print(f"  Last day worked: {last_day}")
                        print("  ---")
            
            # Check categorization
            london_hybrid_mask = (df['Location'] == 'London UK') & (df['Working Status'] == 'Hybrid')
            
            london_hybrid_present = df[
                date_mask & active_mask & london_hybrid_mask & (df['present'] == 'Yes')
            ]['employee_id'].unique()
            print(f"\nLondon+Hybrid employees counted: {len(london_hybrid_present)}")
            
            others_present = df[
                date_mask & active_mask & ~london_hybrid_mask & (df['present'] == 'Yes')
            ]['employee_id'].unique()
            print(f"Other employees counted: {len(others_present)}")
            
            # Check for NaN values in Location or Working Status
            nan_location = df[date_mask & active_mask & (df['present'] == 'Yes') & df['Location'].isna()]['employee_id'].unique()
            nan_working = df[date_mask & active_mask & (df['present'] == 'Yes') & df['Working Status'].isna()]['employee_id'].unique()
            print(f"Employees with NaN Location: {len(nan_location)}")
            print(f"Employees with NaN Working Status: {len(nan_working)}")
            
            # Total employees that should be counted
            should_be_counted = set(london_hybrid_present) | set(others_present)
            missing = set(active_present) - should_be_counted
            print(f"MISSING EMPLOYEES: {len(missing)}")
            if missing:
                print("\nSample of missing employees:")
                for emp_id in list(missing)[:5]:
                    emp_rows = df[(df['employee_id'] == emp_id) & date_mask]
                    if not emp_rows.empty:
                        emp_row = emp_rows.iloc[0]
                        print(f"  ID: {emp_id}, Name: {emp_row.get('Last name, First name', 'N/A')}")
                        print(f"  Location: {emp_row.get('Location', 'N/A')}, Working Status: {emp_row.get('Working Status', 'N/A')}")
                        print(f"  Present: {emp_row.get('present', 'N/A')}, Combined hire date: {emp_row.get('Combined hire date', 'N/A')}")
                        print(f"  Most recent day worked: {emp_row.get('Most recent day worked', 'N/A')}")
                        print("  ---")
        
        # Continue with the original function calculation
        # Consider employment dates
        active_mask = (
            (pd.to_datetime(df['Combined hire date']) <= date) & 
            ((df['Most recent day worked'].isna()) | (pd.to_datetime(df['Most recent day worked']) >= date))
        )
        
        london_hybrid_mask = (df['Location'] == 'London UK') & (df['Working Status'] == 'Hybrid')
        
        # Count London + Hybrid who were present
        london_hybrid_count = df[
            date_mask & active_mask & london_hybrid_mask & (df['present'] == 'Yes')
        ]['employee_id'].nunique()
        
        # Count others who were present
        others_count = df[
            date_mask & active_mask & ~london_hybrid_mask & (df['present'] == 'Yes')
        ]['employee_id'].nunique()
        
        # Get total eligible London + Hybrid employees for that date
        total_eligible_london_hybrid = df[
            active_mask & london_hybrid_mask
        ]['employee_id'].nunique()
        
        # Calculate percentage based on eligible employees
        attendance_percentage = (
            (london_hybrid_count / total_eligible_london_hybrid * 100)
            if total_eligible_london_hybrid > 0 else 0
        )
        
        daily_counts.append({
            'date': date,
            'day_of_week': pd.Timestamp(date).strftime('%A'),
            'london_hybrid_count': london_hybrid_count,
            'other_count': others_count,
            'eligible_london_hybrid': total_eligible_london_hybrid,
            'london_hybrid_percentage': round(attendance_percentage, 1),
            'total_attendance': london_hybrid_count + others_count
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
                
                london_hybrid_mask = (df['Location'] == 'London UK') & (df['Working Status'] == 'Hybrid')
                
                # Count eligible employees for this day
                eligible_count = df[
                    active_mask &
                    london_hybrid_mask
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
        
        london_hybrid_mask = (df['Location'] == 'London UK') & (df['Working Status'] == 'Hybrid')
        
        # Calculate daily attendance for London+Hybrid
        london_hybrid_daily = df[
            week_mask & 
            active_mask &
            london_hybrid_mask & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique()
        
        # Calculate daily attendance for others
        others_daily = df[
            week_mask & 
            active_mask &
            ~london_hybrid_mask & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique()
        
        # Calculate averages
        london_hybrid_avg = london_hybrid_daily.mean() if not london_hybrid_daily.empty else 0
        others_avg = others_daily.mean() if not others_daily.empty else 0
        
        # Calculate attendance percentage using average eligible employees
        attendance_percentage = (
            (london_hybrid_avg / avg_eligible * 100)
            if avg_eligible > 0 else 0
        )
        
        weekly_counts.append({
            'week_start': week_start,
            'london_hybrid_avg': round(london_hybrid_avg, 1),
            'other_avg': round(others_avg, 1),
            'avg_eligible_london_hybrid': avg_eligible,  # New field
            'london_hybrid_percentage': round(attendance_percentage, 1),
            'total_avg_attendance': round(london_hybrid_avg + others_avg, 1)
        })
    
    return pd.DataFrame(weekly_counts)

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
        london_hybrid_mask = (df['Location'] == 'London UK') & (df['Working Status'] == 'Hybrid')
        
        # Get attendance for this weekday
        london_hybrid_attendance = df[
            day_mask & 
            london_hybrid_mask & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        others_attendance = df[
            day_mask & 
            ~london_hybrid_mask & 
            (df['present'] == 'Yes')
        ].groupby('date_only')['employee_id'].nunique().mean()
        
        # Get eligible London+Hybrid employees
        eligible_london_hybrid = df[
            day_mask &
            london_hybrid_mask
        ]['employee_id'].nunique()
        
        # Calculate percentage
        attendance_percentage = (
            (london_hybrid_attendance / eligible_london_hybrid * 100)
            if eligible_london_hybrid > 0 else 0
        )
        
        weekday_stats.append({
            'weekday': day,
            'london_hybrid_count': round(london_hybrid_attendance, 1),
            'other_count': round(others_attendance, 1),
            'attendance_percentage': round(attendance_percentage, 1)
        })
    
    return pd.DataFrame(weekday_stats)