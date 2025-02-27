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
    
    # Debug step 3: Check location/working status/full-time filtering
    location_mask = (df["Location"] == "London UK")
    working_mask = (df["Working Status"] == "Hybrid")
    full_time_mask = (df["is_full_time"] == True)
    london_hybrid_ft_mask = location_mask & working_mask & full_time_mask
    
    print("\n[DEBUG] Location/Working Status/Full-Time Analysis:")
    print(f"Total rows: {len(df)}")
    print(f"Rows with London, Hybrid, Full-Time: {len(df[london_hybrid_ft_mask])}")
    print(f"Rows with non-London, Hybrid, Full-Time: {len(df[~london_hybrid_ft_mask])}")
    
    # Group value counts for investigation
    print("\nLocation distribution:")
    print(df["Location"].value_counts())
    print("\nWorking Status distribution:")
    print(df["Working Status"].value_counts())
    print("\nFull-Time Status distribution:")
    print(df["is_full_time"].value_counts())
    
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