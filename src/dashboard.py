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
    create_employee_summary,
    calculate_tue_thu_attendance_percentage,
    calculate_daily_attendance_counts,
    calculate_weekly_attendance_counts,
    calculate_period_summary
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

def filter_by_date_range(df: pd.DataFrame, start_date: pd.Timestamp, end_date: pd.Timestamp, date_col: str = 'date') -> pd.DataFrame:
    """Filter DataFrame by date range."""
    return df[
        (df[date_col] >= pd.Timestamp(start_date)) &
        (df[date_col] <= pd.Timestamp(end_date))
    ]

def format_date(date_str):
    """Convert date string to formatted date (e.g., '7th August 2024')"""
    date = pd.to_datetime(date_str)
    
    def ordinal(n):
        if 10 <= n % 100 <= 20:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
        return str(n) + suffix
    
    return f"{ordinal(date.day)} {date.strftime('%B %Y')}"

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
        
        # Date range selector in sidebar
        st.sidebar.header("Date Range Selection")
        min_date = combined_df['date_only'].min()
        max_date = combined_df['date_only'].max()
        
        default_start = min_date
        default_end = max_date
        
        # Quick selection buttons
        st.sidebar.subheader("Quick Select")
        if st.sidebar.button("Last 30 Days"):
            default_start = max_date - pd.Timedelta(days=30)
            default_end = max_date
        if st.sidebar.button("Last 90 Days"):
            default_start = max_date - pd.Timedelta(days=90)
            default_end = max_date
        
        selected_dates = st.sidebar.date_input(
            "Select Date Range",
            value=(default_start, default_end),
            min_value=min_date,
            max_value=max_date
        )
        
        # Ensure we have both start and end dates
        if len(selected_dates) == 2:
            # Convert date objects to timestamps
            start_date = pd.Timestamp(selected_dates[0])
            end_date = pd.Timestamp(selected_dates[1])
            
            # Filter all DataFrames
            filtered_tue_thu = filter_by_date_range(tue_thu_attendance, start_date, end_date)
            filtered_daily = filter_by_date_range(daily_counts, start_date, end_date)
            filtered_weekly = filter_by_date_range(weekly_counts, start_date, end_date, 'week_start')
            
            # Calculate period summary
            period_summary = calculate_period_summary(combined_df, start_date, end_date)
            
            # Create tabs
            tab1, tab2, tab3, tab4 = st.tabs(["Daily Overview", "Weekly Overview", "Period Summary", "Employee Details"])
            
            with tab1:
                st.subheader("Daily Attendance Percentage (Tuesday-Thursday)")
                if len(filtered_tue_thu) > 0:
                    fig_daily_pct = px.line(
                        filtered_tue_thu,
                        x='date',
                        y='percentage',
                        title='Daily Office Attendance Percentage (Tue-Thu)',
                        labels={'percentage': 'Attendance %', 'date': 'Date'}
                    )
                    st.plotly_chart(fig_daily_pct)
                
                # Daily details table
                st.subheader("Daily Attendance Details")
                
                # Format the date column before display
                display_df = filtered_daily.copy()
                display_df['date'] = display_df['date'].apply(format_date)
                
                # Rename columns to be more readable
                column_mapping = {
                    'date': 'Date',
                    'day_of_week': 'Day of Week',
                    'london_hybrid_count': 'London, Hybrid Attendance (#)',
                    'eligible_london_hybrid': 'London, Hybrid (total #)',
                    'london_hybrid_percentage': 'London, Hybrid Attendance (%)',
                    'other_count': 'Non-London, Hybrid Attendance (#)',
                    'total_attendance': 'Total Attendance (#)'
                }
                
                display_df = display_df.rename(columns=column_mapping)
                
                # Format numbers while keeping original values
                count_columns = [
                    'London, Hybrid Attendance (#)',
                    'London, Hybrid (total #)',
                    'Non-London, Hybrid Attendance (#)',
                    'Total Attendance (#)'
                ]
                percentage_columns = ['London, Hybrid Attendance (%)']
                
                # Create a styled version for display
                styled_df = display_df.copy()
                for col in count_columns:
                    styled_df[col] = styled_df[col].apply(lambda x: f"{round(x):,}")
                for col in percentage_columns:
                    styled_df[col] = styled_df[col].apply(lambda x: f"{x:.1f}%")
                
                # Reorder columns
                column_order = [
                    'Date',
                    'Day of Week',
                    'London, Hybrid Attendance (#)',
                    'London, Hybrid (total #)',
                    'London, Hybrid Attendance (%)',
                    'Non-London, Hybrid Attendance (#)',
                    'Total Attendance (#)'
                ]
                styled_df = styled_df[column_order]
                st.dataframe(styled_df, hide_index=True)
            
            with tab2:
                st.subheader("Weekly Attendance Percentage (Tuesday-Thursday only)")
                if len(filtered_weekly) > 0:
                    fig_weekly_pct = px.line(
                        filtered_weekly,
                        x='week_start',
                        y='london_hybrid_percentage',
                        title='Weekly Office Attendance Percentage',
                        labels={
                            'london_hybrid_percentage': 'Attendance %',
                            'week_start': 'Week Starting'
                        }
                    )
                    st.plotly_chart(fig_weekly_pct)
                
                st.subheader("Weekly Attendance Counts (Tuesday-Thursday only)")
                if len(filtered_weekly) > 0:
                    fig_weekly_counts = px.bar(
                        filtered_weekly,
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
                st.subheader("Weekly Attendance Details (Tuesday-Thursday only)")
                display_cols_weekly = {
                    'week_start': 'Week Starting',
                    'london_hybrid_avg': 'Avg. London, Hybrid Attendance (#)',
                    'avg_eligible_london_hybrid': 'Avg. London, Hybrid (total #)',
                    'london_hybrid_percentage': 'Avg. London, Hybrid Attendance (%)',
                    'other_avg': 'Avg. Non-London, Hybrid Attendance (#)',
                    'total_avg_attendance': 'Avg. Total Attendance (#)'
                }
                weekly_display = filtered_weekly[display_cols_weekly.keys()].rename(columns=display_cols_weekly)
                
                # Format the week_start column
                weekly_display['Week Starting'] = weekly_display['Week Starting'].apply(format_date)
                
                # Create a styled version for display
                styled_weekly = weekly_display.copy()
                
                # Format numbers
                count_columns = [
                    'Avg. London, Hybrid Attendance (#)',
                    'Avg. London, Hybrid (total #)',
                    'Avg. Non-London, Hybrid Attendance (#)',
                    'Avg. Total Attendance (#)'
                ]
                percentage_columns = ['Avg. London, Hybrid Attendance (%)']
                
                for col in count_columns:
                    styled_weekly[col] = styled_weekly[col].apply(lambda x: f"{round(x):,}")
                for col in percentage_columns:
                    styled_weekly[col] = styled_weekly[col].apply(lambda x: f"{x:.1f}%")
                
                # Reorder columns
                weekly_column_order = [
                    'Week Starting',
                    'Avg. London, Hybrid Attendance (#)',
                    'Avg. London, Hybrid (total #)',
                    'Avg. London, Hybrid Attendance (%)',
                    'Avg. Non-London, Hybrid Attendance (#)',
                    'Avg. Total Attendance (#)'
                ]
                styled_weekly = styled_weekly[weekly_column_order]
                st.dataframe(styled_weekly, hide_index=True)
            
            with tab3:
                st.subheader("Period Summary")
                if len(period_summary) > 0:
                    fig_period = px.bar(
                        period_summary,
                        x='weekday',
                        y=['london_hybrid_count', 'other_count'],
                        title='Average Daily Attendance by Weekday',
                        labels={
                            'weekday': 'Day of Week',
                            'value': 'Average Attendance',
                            'variable': 'Employee Type'
                        },
                        barmode='group'
                    )
                    
                    fig_period.update_traces(
                        name='London + Hybrid',
                        selector=dict(name='london_hybrid_count')
                    )
                    fig_period.update_traces(
                        name='Other Employees',
                        selector=dict(name='other_count')
                    )
                    
                    st.plotly_chart(fig_period)
                    
                    # Period summary table
                    st.subheader("Weekday Averages")
                    display_cols_period = {
                        'weekday': 'Day of Week',
                        'london_hybrid_count': 'Avg. London + Hybrid Count',
                        'other_count': 'Avg. Other Count',
                        'attendance_percentage': 'London + Hybrid Attendance %'
                    }
                    period_display = period_summary[display_cols_period.keys()].rename(columns=display_cols_period)
                    
                    # Create styled version
                    styled_period = period_display.copy()
                    
                    # Format numbers
                    count_columns = ['Avg. London + Hybrid Count', 'Avg. Other Count']
                    percentage_columns = ['London + Hybrid Attendance %']
                    
                    for col in count_columns:
                        styled_period[col] = styled_period[col].apply(lambda x: f"{round(x):,}")
                    for col in percentage_columns:
                        styled_period[col] = styled_period[col].apply(lambda x: f"{x:.1f}%")
                    
                    st.dataframe(styled_period, hide_index=True)
            
            with tab4:
                st.subheader("Employee Attendance Summary")
                
                # Filter employee summary for selected date range
                date_filtered_df = combined_df[
                    (combined_df['date_only'] >= start_date) &
                    (combined_df['date_only'] <= end_date)
                ]
                
                # Get employee summary with friendly column headers
                filtered_employee_summary = create_employee_summary(date_filtered_df)
                
                # Display the table (no additional renaming needed as it's done in create_employee_summary)
                st.dataframe(filtered_employee_summary, hide_index=True)
        
        else:
            st.error("Please select both start and end dates")
            
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        print(f"Error details: {e}")

if __name__ == "__main__":
    main()
