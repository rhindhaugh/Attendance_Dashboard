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
    calculate_weekly_tue_thu_attendance,
    calculate_tue_thu_attendance_percentage,
    calculate_daily_attendance_counts,
    calculate_weekly_attendance_counts
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
        tue_thu_attendance = calculate_tue_thu_attendance_percentage(combined_df)
        daily_counts = calculate_daily_attendance_counts(combined_df)
        weekly_counts = calculate_weekly_attendance_counts(combined_df)
        employee_summary = create_employee_summary(combined_df)
        
        # Create tabs
        tab1, tab2, tab3 = st.tabs(["Daily Overview", "Weekly Overview", "Employee Details"])
        
        with tab1:
            # Main attendance percentage chart (Tue-Thu only)
            st.subheader("Daily Attendance Percentage (Tuesday-Thursday)")
            fig_daily_pct = px.line(
                tue_thu_attendance,
                x='date',
                y='percentage',
                title='Daily Office Attendance Percentage (Tue-Thu)',
                labels={'percentage': 'Attendance %', 'date': 'Date'}
            )
            st.plotly_chart(fig_daily_pct)
            
            # Daily attendance counts
            st.subheader("Daily Attendance Counts")
            fig_daily_counts = px.bar(
                daily_counts,
                x='date',
                y=['london_hybrid_count', 'other_count'],
                title='Daily Attendance by Employee Type',
                labels={
                    'date': 'Date',
                    'value': 'Number of Employees',
                    'variable': 'Employee Type'
                },
                barmode='stack'
            )
            
            # Update legend labels
            fig_daily_counts.update_traces(
                name='London + Hybrid',
                selector=dict(name='london_hybrid_count')
            )
            fig_daily_counts.update_traces(
                name='Other Employees',
                selector=dict(name='other_count')
            )
            
            st.plotly_chart(fig_daily_counts)
            
            # Detailed daily table
            st.subheader("Daily Attendance Details")
            
            # Filter for weekdays
            weekday_data = daily_counts[
                ~daily_counts['day_of_week'].isin(['Saturday', 'Sunday'])
            ].copy()
            
            # Format for display
            display_cols = {
                'date': 'Date',
                'day_of_week': 'Day',
                'london_hybrid_count': 'London + Hybrid Count',
                'other_count': 'Other Count',
                'london_hybrid_percentage': 'London + Hybrid %'
            }
            
            weekday_data = weekday_data[display_cols.keys()].rename(columns=display_cols)
            st.dataframe(weekday_data, hide_index=True)
        
        with tab2:
            # Weekly attendance percentage
            st.subheader("Weekly Attendance Percentage")
            fig_weekly_pct = px.line(
                weekly_counts,
                x='week_start',
                y='london_hybrid_percentage',
                title='Weekly Office Attendance Percentage',
                labels={
                    'london_hybrid_percentage': 'Attendance %',
                    'week_start': 'Week Starting'
                }
            )
            st.plotly_chart(fig_weekly_pct)
            
            # Weekly attendance counts
            st.subheader("Weekly Attendance Counts")
            fig_weekly_counts = px.bar(
                weekly_counts,
                x='week_start',
                y=['london_hybrid_avg', 'other_avg'],
                title='Average Daily Attendance by Employee Type (Weekly)',
                labels={
                    'week_start': 'Week Starting',
                    'value': 'Average Daily Attendance',
                    'variable': 'Employee Type'
                },
                barmode='stack'
            )
            
            # Update legend labels
            fig_weekly_counts.update_traces(
                name='London + Hybrid',
                selector=dict(name='london_hybrid_avg')
            )
            fig_weekly_counts.update_traces(
                name='Other Employees',
                selector=dict(name='other_avg')
            )
            
            st.plotly_chart(fig_weekly_counts)
            
            # Weekly details table
            st.subheader("Weekly Attendance Details")
            display_cols_weekly = {
                'week_start': 'Week Starting',
                'london_hybrid_avg': 'Avg. London + Hybrid Count',
                'other_avg': 'Avg. Other Count',
                'london_hybrid_percentage': 'London + Hybrid %',
                'total_avg_attendance': 'Total Avg. Attendance'
            }
            weekly_display = weekly_counts[display_cols_weekly.keys()].rename(columns=display_cols_weekly)
            st.dataframe(weekly_display, hide_index=True)
            
        with tab3:
            st.subheader("Employee Attendance Summary")
            st.dataframe(employee_summary, hide_index=True)
            
            # Add option to show raw data
            if st.checkbox("Show Raw Attendance Data"):
                st.subheader("Raw Daily Attendance")
                st.write(daily_counts)
            
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        print(f"Error details: {e}")

if __name__ == "__main__":
    main()
