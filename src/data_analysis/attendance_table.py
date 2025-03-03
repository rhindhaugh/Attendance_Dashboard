import pandas as pd

def build_attendance_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build attendance table from key card data.
    """
    df = df.copy()
    
    # Make sure parsed_time exists
    if 'parsed_time' not in df.columns:
        if 'Date/time' in df.columns:
            try:
                df['parsed_time'] = pd.to_datetime(df['Date/time'], dayfirst=True, errors='coerce')
                print("\nAdded missing parsed_time column from Date/time")
            except Exception as e:
                print(f"\nERROR converting Date/time to parsed_time: {e}")
                # Check if there's a Date_Parsed column we can use instead
                if 'Date_Parsed' in df.columns:
                    print("Using Date_Parsed column instead")
                    df['parsed_time'] = pd.to_datetime(df['Date_Parsed'], errors='coerce')
                else:
                    print("No valid time information found. Setting parsed_time to NaT")
                    df['parsed_time'] = pd.NaT
        elif 'Date_Parsed' in df.columns:
            print("\nUsing Date_Parsed column for parsed_time")
            df['parsed_time'] = pd.to_datetime(df['Date_Parsed'], errors='coerce')
        else:
            print("\nWARNING: No time information found. Cannot calculate arrival times.")
            df['parsed_time'] = pd.NaT
    
    # Debug step 1: Only show a sample of scans (not from Livia)
    print("\n[DEBUG] Sample earliest scans per day per employee:")
    temp = (
        df
        .sort_values(["employee_id", "date_only", "parsed_time"])
        .groupby(["employee_id", "date_only"])
        .head(3)
    )
    
    # Try to find an employee that isn't Livia
    non_livia = temp[~temp["Last name, First name"].str.contains("Livia", na=False)]
    
    if len(non_livia) > 0:
        # Get one employee who isn't Livia
        sample_emp_id = non_livia["employee_id"].iloc[0]
        sample_data = temp[temp["employee_id"] == sample_emp_id]
        print(sample_data[["employee_id", "Last name, First name", "date_only", "parsed_time", "Where"]].head(3))
    else:
        # If somehow everyone is Livia or no data, just show minimal sample
        print(temp[["employee_id", "date_only", "parsed_time", "Where"]].head(3))
    
    # Apply filters but minimize debug output
    location_mask = (df["Location"] == "London UK")
    working_mask = (df["Working Status"] == "Hybrid")
    
    # Handle missing is_full_time column
    if "is_full_time" in df.columns:
        full_time_mask = (df["is_full_time"] == True)
    else:
        print("WARNING: is_full_time column not found. Assuming all employees are full-time.")
        full_time_mask = pd.Series(True, index=df.index)
        
    london_hybrid_ft_mask = location_mask & working_mask & full_time_mask
    
    # Minimal filtering summary
    print("\n[DEBUG] Filtering Summary:")
    print(f"Total rows: {len(df)}")
    print(f"Rows with London, Hybrid, Full-Time: {len(df[london_hybrid_ft_mask])}")
    
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
    merged["is_present"] = merged["visits"] > 0  # Use boolean True/False
    # Keep present as a string for backward compatibility 
    merged["present"] = merged["visits"].map({0: "No"}).fillna("Yes")
    
    # Debug step 2: Show rows marked as 'present' (using different employee for example)
    print("\n[DEBUG] Checking presence logic. Sample 'present' rows:")
    present_only = merged[merged["is_present"] == True]  # Using boolean True
    
    # Try to find an employee that isn't Livia for the example
    sample_employees = present_only["employee_name"].unique()
    alt_employee = None
    
    # Look for someone who isn't Livia
    for emp in sample_employees:
        if "Livia" not in emp:
            alt_employee = emp
            break
    
    # If found someone else, filter for them, otherwise use default sample
    if alt_employee:
        sample_rows = present_only[present_only["employee_name"] == alt_employee]
        print(sample_rows[["employee_id", "employee_name", "date_only", "is_present", "visits"]].head(3))
    else:
        print(present_only[["employee_id", "employee_name", "date_only", "is_present", "visits"]].head(3))
    
    # 4) Calculate total days attended per employee
    days_attended = (
        merged[merged["is_present"] == True]  # Use boolean is_present
        .groupby("employee_id")
        .size()
        .reset_index(name="days_attended")
    )
    
    # Merge days_attended back to our main table
    final_df = merged.merge(days_attended, on="employee_id", how="left")
    
    # Sort by employee name and date
    final_df = final_df.sort_values(["employee_name", "date_only"])
    
    return final_df