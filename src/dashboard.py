import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import time
import gc  # For garbage collection
import altair as alt

# Data cleaning and ingestion imports
from data_ingestion import (
    load_key_card_data,
    load_employee_info,
    calculate_default_date_range
)
from data_cleaning import (
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

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data(start_date=None, end_date=None, last_n_days=None):
    """
    Load and preprocess the data with caching.
    
    Args:
        start_date: Optional start date string in format 'YYYY-MM-DD'
        end_date: Optional end date string in format 'YYYY-MM-DD'
        last_n_days: If provided, load only the last N days of data
        
    Returns:
        Tuple of (key_card_df, employee_df)
    """
    start_time = time.time()
    
    key_card_path = Path("data/raw/key_card_access.csv")
    key_card_df = load_key_card_data(
        str(key_card_path), 
        start_date=start_date, 
        end_date=end_date, 
        last_n_days=last_n_days
    )
    print(f"\nLoaded key card data: {len(key_card_df)} rows, "
          f"from {key_card_df['Date/time'].min()} to {key_card_df['Date/time'].max()}")
    
    employee_path = Path("data/raw/employee_info.csv")
    employee_df = load_employee_info(str(employee_path))
    print(f"\nLoaded employee data: {len(employee_df)} rows")
    
    print(f"Data loading completed in {time.time() - start_time:.2f} seconds")
    
    return key_card_df, employee_df

@st.cache_data(ttl=3600)  # Cache for 1 hour
def process_data(key_card_df, employee_df):
    """Process and merge data with caching."""
    start_time = time.time()
    
    # Clean key card data first
    key_card_df = clean_key_card_data(key_card_df)
    
    # Get the maximum date from key card data
    max_data_date = key_card_df['date_only'].max()
    print(f"\nMaximum date in key card data: {max_data_date}")
    
    # Clean employee data, passing the max_data_date
    employee_df = clean_employee_info(employee_df, max_data_date)
    
    # Merge the datasets
    combined_df = merge_key_card_with_employee_info(key_card_df, employee_df)
    
    # Clean up memory
    del key_card_df
    gc.collect()
    
    print(f"Data processing completed in {time.time() - start_time:.2f} seconds")
    
    return combined_df

@st.cache_data(ttl=3600)  # Cache for 1 hour
def calculate_analyses(combined_df, start_date=None, end_date=None):
    """Calculate all analyses with caching."""
    start_time = time.time()
    
    if start_date and end_date:
        date_mask = (
            (combined_df['date_only'] >= pd.to_datetime(start_date)) &
            (combined_df['date_only'] <= pd.to_datetime(end_date))
        )
        filtered_df = combined_df[date_mask]
    else:
        filtered_df = combined_df
    
    # Create attendance table
    attendance_table = build_attendance_table(filtered_df)
    
    # CRITICAL FIX: Merge attendance data back to filtered_df, just like in load_and_process_data
    filtered_df = filtered_df.merge(
        attendance_table[['employee_id', 'date_only', 'present', 'visits']],
        on=['employee_id', 'date_only'],
        how='left'
    )
    
    # Fill any missing values in present column with 'No'
    filtered_df['present'] = filtered_df['present'].fillna('No')
    
    # Calculate all analyses
    tue_thu_attendance = calculate_tue_thu_attendance_percentage(filtered_df)
    daily_counts = calculate_daily_attendance_counts(filtered_df)
    weekly_counts = calculate_weekly_attendance_counts(filtered_df)
    period_summary = calculate_period_summary(filtered_df, 
                                           pd.to_datetime(start_date) if start_date else None,
                                           pd.to_datetime(end_date) if end_date else None)
    employee_summary = create_employee_summary(filtered_df)
    
    # Clean up memory
    del filtered_df
    gc.collect()
    
    print(f"Analysis calculations completed in {time.time() - start_time:.2f} seconds")
    
    return {
        'attendance_table': attendance_table,
        'tue_thu_attendance': tue_thu_attendance,
        'daily_counts': daily_counts,
        'weekly_counts': weekly_counts,
        'period_summary': period_summary,
        'employee_summary': employee_summary
    }

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
    
    # Clean key card data first
    key_card_df = clean_key_card_data(key_card_df)
    
    # Get the maximum date from key card data
    max_data_date = key_card_df['date_only'].max()
    print(f"\nMaximum date in key card data: {max_data_date}")
    
    # Clean employee data, passing the max_data_date
    employee_df = clean_employee_info(employee_df, max_data_date)
    
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
    
    return combined_df, attendance_table

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
    
    # Add loading time warning
    st.sidebar.warning(
        "Note: Larger date ranges will take longer to load and process."
    )
    
    st.sidebar.header("Data Range Selection")
    
    # Get the most recent date in the data
    key_card_df = pd.read_csv("data/raw/key_card_access.csv")
    most_recent_date = pd.to_datetime(key_card_df['Date/time'], dayfirst=True).max()
    del key_card_df  # Clean up memory
    
    data_range_option = st.sidebar.radio(
        "Select data range to analyze:",
        [
            "Year to Date",
            "Last 30 Days",
            "Last 3 Months",
            "Last 6 Months",
            "2023 Full Year",
            "2024 Full Year",
            "Custom Date Range"
        ]
    )
    
    # Set date parameters based on selection
    if data_range_option == "Year to Date":
        st.sidebar.info(
            f"Data shown is for the one-year period ending {most_recent_date.strftime('%d %B %Y')}, "
            "which is the most recent data available."
        )
        start_date = (most_recent_date - pd.Timedelta(days=365)).strftime("%Y-%m-%d")
        end_date = most_recent_date.strftime("%Y-%m-%d")
        last_n_days = None
        
    elif data_range_option == "Last 30 Days":
        start_date = None
        end_date = None
        last_n_days = 30
        
    elif data_range_option == "Last 3 Months":
        start_date = (most_recent_date - pd.Timedelta(days=90)).strftime("%Y-%m-%d")
        end_date = most_recent_date.strftime("%Y-%m-%d")
        last_n_days = None
        
    elif data_range_option == "Last 6 Months":
        start_date = (most_recent_date - pd.Timedelta(days=180)).strftime("%Y-%m-%d")
        end_date = most_recent_date.strftime("%Y-%m-%d")
        last_n_days = None
        
    elif data_range_option == "2023 Full Year":
        start_date = "2023-01-01"
        end_date = "2023-12-31"
        last_n_days = None
        
    elif data_range_option == "2024 Full Year":
        start_date = "2024-01-01"
        end_date = "2024-12-31"
        last_n_days = None
        
    else:  # Custom Date Range
        default_start = most_recent_date - pd.Timedelta(days=30)
        date_range = st.sidebar.date_input(
            "Select date range",
            value=(default_start, most_recent_date),
            min_value=pd.to_datetime("2023-01-01"),
            max_value=most_recent_date
        )
        if len(date_range) == 2:
            start_date = date_range[0].strftime("%Y-%m-%d")
            end_date = date_range[1].strftime("%Y-%m-%d")
        else:
            st.error("Please select both start and end dates")
            return
        last_n_days = None

    data_load_state = st.text("Loading data... This may take a moment.")
    
    try:
        key_card_df, employee_df = load_data(start_date, end_date, last_n_days)
        data_load_state.text("Processing data...")
        combined_df = process_data(key_card_df, employee_df)
        
        del key_card_df
        del employee_df
        gc.collect()
        
        data_load_state.text("Calculating analytics...")
        analyses = calculate_analyses(combined_df, start_date, end_date)
        data_load_state.empty()
        
        min_date = combined_df['date_only'].min()
        max_date = combined_df['date_only'].max()
        st.success(f"Loaded {len(combined_df):,} records from {min_date.strftime('%d %b %Y')} to {max_date.strftime('%d %b %Y')}")
        
        # Create tabs and display data (keep existing tab code, but use analyses dict)
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Daily Overview", 
            "Weekly Overview", 
            "Period Summary", 
            "Employee Details",
            "Employee Data"
        ])
        
        with tab1:
            st.subheader("Daily Attendance Percentage (Tuesday-Thursday)")
            if len(analyses['tue_thu_attendance']) > 0:
                fig_daily_pct = px.line(
                    analyses['tue_thu_attendance'],
                    x='date',
                    y='percentage',
                    title='Daily Office Attendance Percentage (Tue-Thu)',
                    labels={'percentage': 'Attendance %', 'date': 'Date'}
                )
                st.plotly_chart(fig_daily_pct)
            
            # Add daily office attendance count chart (Tue-Thu only)
            st.subheader("Daily Office Attendance (Count) - Tuesdays to Thursdays")
            
            # Filter for only Tue-Thu
            tue_thu_daily = analyses['daily_counts'][
                analyses['daily_counts']['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])
            ]
            
            if len(tue_thu_daily) > 0:
                fig_daily_counts = px.bar(
                    tue_thu_daily,
                    x='date',
                    y=['other_count', 'london_hybrid_count'],  # Order matters for stacking - other on top
                    title='Daily Attendance by Employee Type',
                    labels={
                        'date': 'Date',
                        'value': 'Attendance Count',
                        'variable': 'Employee Type'
                    },
                    barmode='stack'
                )
                
                # Update trace names to be more readable
                fig_daily_counts.update_traces(
                    name='London, Hybrid',
                    selector=dict(name='london_hybrid_count')
                )
                fig_daily_counts.update_traces(
                    name='Other Employees',
                    selector=dict(name='other_count')
                )
                
                # Update x-axis to show month and year format
                fig_daily_counts.update_xaxes(
                    tickformat="%b %Y",
                    tickangle=-45
                )
                
                st.plotly_chart(fig_daily_counts, use_container_width=True)
            
            # Daily details table
            st.subheader("Daily Attendance Details")
            
            # Format the date column before display
            display_df = analyses['daily_counts'].copy()
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
            if len(analyses['weekly_counts']) > 0:
                fig_weekly_pct = px.line(
                    analyses['weekly_counts'],
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
            if len(analyses['weekly_counts']) > 0:
                fig_weekly_counts = px.bar(
                    analyses['weekly_counts'],
                    x='week_start',
                    y=['other_avg', 'london_hybrid_avg'],  # Order matters for stacking - other on top
                    title='Average Daily Attendance by Employee Type (Weekly)',
                    labels={
                        'week_start': 'Week Starting',
                        'value': 'Average Daily Attendance',
                        'variable': 'Employee Type'
                    },
                    barmode='stack'
                )
                
                fig_weekly_counts.update_traces(
                    name='London, Hybrid',
                    selector=dict(name='london_hybrid_avg')
                )
                fig_weekly_counts.update_traces(
                    name='Other Employees',
                    selector=dict(name='other_avg')
                )
                
                # Update x-axis to show month and year format
                fig_weekly_counts.update_xaxes(
                    tickformat="%b %Y",
                    tickangle=-45
                )
                
                st.plotly_chart(fig_weekly_counts, use_container_width=True)
            
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
            weekly_display = analyses['weekly_counts'][display_cols_weekly.keys()].rename(columns=display_cols_weekly)
            
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
            if len(analyses['period_summary']) > 0:
                fig_period = px.bar(
                    analyses['period_summary'],
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
                period_display = analyses['period_summary'][display_cols_period.keys()].rename(columns=display_cols_period)
                
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
            
            # Filter employee summary for selected date range only if both dates are provided
            if start_date and end_date:
                date_filtered_df = combined_df[
                    (combined_df['date_only'] >= pd.to_datetime(start_date)) &
                    (combined_df['date_only'] <= pd.to_datetime(end_date))
                ]
            else:
                date_filtered_df = combined_df  # Use all data if no date range
            
            # Get employee summary with friendly column headers
            filtered_employee_summary = analyses['employee_summary']
            
            # Display the table (no additional renaming needed as it's done in create_employee_summary)
            st.dataframe(filtered_employee_summary, hide_index=True)
        
        with tab5:
            st.subheader("Transformed Employee Data")
            
            # Get the cleaned employee data
            # Load the employee data directly
            employee_df = load_employee_info("data/raw/employee_info.csv")
            
            # Get the maximum date from the combined data for proper date handling
            max_data_date = combined_df['date_only'].max() if 'date_only' in combined_df.columns else None
            
            # Clean the employee data with the max date
            employee_df = clean_employee_info(employee_df, max_data_date)
            
            # Keep only relevant columns used in calculations
            relevant_columns = [
                'employee_id',
                'Last name, First name',
                'Working Status',
                'Location',
                'Division',
                'Department',
                'Combined hire date',
                'Most recent day worked'
            ]
            
            # Filter to only include columns that exist
            available_columns = [col for col in relevant_columns if col in employee_df.columns]
            display_df = employee_df[available_columns].copy()
            
            # Rename columns for display
            column_mapping = {
                'employee_id': 'Employee ID',
                'Last name, First name': 'Employee Name',
                'Working Status': 'Working Status',
                'Location': 'Location',
                'Division': 'Division',
                'Department': 'Department',
                'Combined hire date': 'Hire Date',
                'Most recent day worked': 'Last Day'
            }
            
            # Only rename columns that exist
            display_df = display_df.rename(columns={k: v for k, v in column_mapping.items() if k in display_df.columns})
            
            # Format dates
            date_columns = ['Hire Date', 'Last Day']
            for col in date_columns:
                if col in display_df.columns:
                    display_df[col] = pd.to_datetime(display_df[col]).dt.strftime('%d/%m/%Y')
            
            # Sort by Employee Name if it exists
            if 'Employee Name' in display_df.columns:
                display_df = display_df.sort_values('Employee Name')
            
            # Display the table
            st.dataframe(display_df, hide_index=True)
            
            # Add download button
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="Download Employee Data as CSV",
                data=csv,
                file_name="employee_data.csv",
                mime="text/csv"
            )
    
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        print(f"Error details: {e}")

if __name__ == "__main__":
    main()
