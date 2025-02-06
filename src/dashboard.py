import streamlit as st
import pandas as pd
import plotly.express as px

from data_ingestion import load_key_card_data, load_employee_info
from data_cleaning import (
    clean_key_card_data, 
    clean_employee_info, 
    merge_key_card_with_employee_info,
    add_time_analysis_columns
)
from data_analysis import (
    build_attendance_table,
    calculate_visit_counts,
    calculate_average_arrival_hour
)

def main():
    st.title("Office Attendance Dashboard")

    # Step 1: Load Data
    key_card_df = load_key_card_data("data/raw/key_card_access.csv")
    employee_df = load_employee_info("data/raw/employee_info.csv")

    # Step 2: Clean & merge
    key_card_df = clean_key_card_data(key_card_df)
    key_card_df = add_time_analysis_columns(key_card_df)
    employee_df = clean_employee_info(employee_df)
    merged_df = merge_key_card_with_employee_info(key_card_df, employee_df)

    # Step 3: Analysis
    attendance_table = build_attendance_table(merged_df)
    
    # Display attendance analysis
    st.subheader("Employee Attendance")
    
    # Show total days attended summary
    days_summary = (
        attendance_table[["employee_name", "days_attended"]]
        .drop_duplicates()
        .sort_values("days_attended", ascending=False)
    )
    
    st.write("Total Days Attended by Employee")
    st.dataframe(days_summary)
    
    # Create bar chart of attendance
    fig = px.bar(
        days_summary,
        x="employee_name",
        y="days_attended",
        title="Days Attended by Employee"
    )
    fig.update_layout(
        xaxis_title="Employee",
        yaxis_title="Days Attended",
        xaxis_tickangle=45
    )
    st.plotly_chart(fig)
    
    # Show detailed attendance table
    st.write("Detailed Daily Attendance")
    st.dataframe(attendance_table)

if __name__ == "__main__":
    main()
