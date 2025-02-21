import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Data cleaning and ingestion imports
from data_cleaning import (
    load_key_card_data,
    load_employee_info,
    clean_key_card_data,
    clean_employee_info,
    merge_key_card_with_employee_info,
    add_time_analysis_columns
)

# Data analysis imports
from data_analysis import (
    build_attendance_table,
    calculate_visit_counts,
    calculate_average_arrival_hour,
    calculate_daily_attendance_percentage,
    calculate_weekly_attendance_percentage,
    calculate_attendance_by_weekday,
    calculate_attendance_by_division,
    calculate_individual_attendance,
    analyze_entrance_patterns,
    create_employee_summary,
    calculate_weekday_attendance,
    calculate_weekly_tue_thu_attendance
)

def load_data():
    """Load and preprocess the data."""
    # Load key card data
    key_card_path = Path("data/raw/key_card_access.csv")
    key_card_df = pd.read_csv(key_card_path)
    print("\nLoaded key card data columns:", key_card_df.columns.tolist())
    
    # Load employee info
    employee_path = Path("data/raw/employee_info.csv")
    employee_df = pd.read_csv(employee_path)
    print("\nLoaded employee info columns:", employee_df.columns.tolist())
    
    return key_card_df, employee_df

def save_processed_data(attendance_table, daily_attendance_pct, avg_arrival_hours):
    """Save processed data to CSV files."""
    # Create processed data directory if it doesn't exist
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Save each DataFrame
    attendance_table.to_csv(processed_dir / "attendance_table.csv", index=False)
    daily_attendance_pct.to_csv(processed_dir / "days_summary.csv", index=False)
    avg_arrival_hours.to_csv(processed_dir / "avg_arrival_hours.csv", index=False)

def load_and_process_data():
    """Load and process all data, returning the combined DataFrame."""
    # Load raw data
    key_card_df, employee_df = load_data()
    
    # Clean data
    key_card_df = clean_key_card_data(key_card_df)
    employee_df = clean_employee_info(employee_df)
    
    # Merge datasets
    combined_df = merge_key_card_with_employee_info(key_card_df, employee_df)
    
    # Create attendance table to get 'present' field
    attendance_table = build_attendance_table(combined_df)
    
    # Merge attendance data back to combined_df
    combined_df = combined_df.merge(
        attendance_table[['employee_id', 'date_only', 'present', 'visits']],
        on=['employee_id', 'date_only'],
        how='left'
    )
    
    # Fill any missing values in present column with 'No'
    combined_df['present'] = combined_df['present'].fillna('No')
    
    # Print debug info
    print("\nProcessed data info:")
    print(f"Total rows: {len(combined_df)}")
    print(f"Rows with present=Yes: {(combined_df['present'] == 'Yes').sum()}")
    print(f"Unique employees: {combined_df['employee_id'].nunique()}")
    print("\nSample of processed data:")
    print(combined_df[['employee_id', 'date_only', 'present', 'visits', 'Location', 'Working Status']].head())
    
    return combined_df

def main():
    """Main function to run the dashboard."""
    st.title("Office Attendance Dashboard")
    
    try:
        # Load and process data
        combined_df = load_and_process_data()
        
        # Create analyses
        employee_summary = create_employee_summary(combined_df)
        daily_attendance = calculate_daily_attendance_percentage(combined_df)
        weekday_attendance = calculate_weekday_attendance(combined_df)
        weekly_tue_thu = calculate_weekly_tue_thu_attendance(combined_df)
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["Daily Overview", "Attendance Patterns", "Employee Summary"])
        
        with tab1:
            st.subheader("Daily Attendance Percentage")
            fig_daily = px.line(
                daily_attendance,
                x='date',
                y='percentage',
                title='Daily Office Attendance Percentage'
            )
            st.plotly_chart(fig_daily)
        
        with tab2:
            # Weekday Average Attendance
            st.subheader("Average Attendance by Day of Week")
            
            # Create grouped bar chart using plotly
            weekday_fig = px.bar(
                weekday_attendance,
                x='day_of_week',
                y=['london_hybrid_avg', 'others_avg'],
                title='Average Daily Attendance by Employee Type',
                labels={
                    'day_of_week': 'Day of Week',
                    'value': 'Average Attendance',
                    'variable': 'Employee Type'
                },
                barmode='group'
            )
            
            # Update legend labels
            weekday_fig.update_traces(
                name='London + Hybrid',
                selector=dict(name='london_hybrid_avg')
            )
            weekday_fig.update_traces(
                name='Other Employees',
                selector=dict(name='others_avg')
            )
            
            st.plotly_chart(weekday_fig)
            
            # Weekly Tue-Thu Average Attendance
            st.subheader("Weekly Average Attendance (Tue-Thu Only)")
            
            weekly_fig = px.bar(
                weekly_tue_thu,
                x='week_start',
                y='avg_attendance',
                title='Average Weekly Attendance (Tuesdays-Thursdays)',
                labels={
                    'week_start': 'Week Starting',
                    'avg_attendance': 'Average Attendance'
                }
            )
            
            # Customize weekly chart
            weekly_fig.update_xaxes(
                tickangle=45,
                tickformat='%Y-%m-%d'
            )
            
            st.plotly_chart(weekly_fig)
            
            # Option to show raw data
            if st.checkbox("Show Raw Data"):
                st.subheader("Weekday Averages")
                st.write(weekday_attendance)
                
                st.subheader("Weekly Averages (Tue-Thu)")
                st.write(weekly_tue_thu)
        
        with tab3:
            st.subheader("Employee Attendance Summary")
            st.dataframe(employee_summary)
            
            if st.checkbox("Show Raw Data Tables"):
                st.subheader("Daily Attendance Percentages")
                st.write(daily_attendance)
            
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        print(f"Error details: {e}")

if __name__ == "__main__":
    main()
