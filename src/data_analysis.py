import pandas as pd

def build_attendance_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame with at least 'employee_id' and 'date_only' columns,
    returns a table showing attendance for each employee on each date.
    
    Returns DataFrame with columns:
    - employee_id
    - employee_name (from 'Last name, First name')
    - date_only
    - present (Yes/No)
    - days_attended (total # of days that employee showed up)
    """
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
    
    # Convert NaN visits to 0 and mark presence
    merged["visits"] = merged["visits"].fillna(0)
    merged["present"] = merged["visits"].map({0: "No"}).fillna("Yes")
    
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
    
    # Keep only the columns we want
    return final_df[[
        "employee_id",
        "employee_name",
        "date_only",
        "present",
        "days_attended"
    ]]

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
    """
    Calculate average hour of arrival for each employee_id.
    Uses the 'timestamp' column (renamed from 'Date/time' during cleaning).
    """
    df = df.copy()
    # Use 'timestamp' instead of 'Date/time' to match cleaned data
    df["arrival_hour"] = df["timestamp"].dt.hour
    return (
        df.groupby("employee_id")["arrival_hour"]
        .mean()
        .reset_index(name="avg_arrival_hour")
    )
