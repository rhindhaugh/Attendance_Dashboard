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
    calculate_default_date_range,
    load_employment_history
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
    calculate_division_attendance_tue_thu,
    calculate_division_attendance_by_location,
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
        Tuple of (key_card_df, employee_df, history_df)
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
    
    # Load employment history data
    history_path = Path("/Users/rob.hindhaugh/Documents/GitHub/Attendance_Dashboard/data/raw/employment_status_history.csv")
    history_df = load_employment_history(str(history_path))
    print(f"\nLoaded employment history data: {len(history_df)} rows")
    
    print(f"Data loading completed in {time.time() - start_time:.2f} seconds")
    
    return key_card_df, employee_df, history_df

@st.cache_data(ttl=3600)  # Cache for 1 hour
def process_data(key_card_df, employee_df, history_df=None):
    """Process and merge data with caching."""
    start_time = time.time()
    
    # Clean key card data first
    key_card_df = clean_key_card_data(key_card_df)
    
    # Get the maximum date from key card data
    max_data_date = key_card_df['date_only'].max()
    print(f"\nMaximum date in key card data: {max_data_date}")
    
    # Clean employee data, passing the max_data_date
    employee_df = clean_employee_info(employee_df, max_data_date)
    
    # Merge the datasets, including employment history if provided
    combined_df = merge_key_card_with_employee_info(key_card_df, employee_df, history_df)
    
    # Clean up memory
    del key_card_df
    gc.collect()
    
    print(f"Data processing completed in {time.time() - start_time:.2f} seconds")
    
    return combined_df

@st.cache_data(ttl=3600)  # Cache for 1 hour
def calculate_analyses(combined_df, start_date=None, end_date=None):
    """Calculate all analyses with caching."""
    start_time = time.time()
    
    # Ensure date columns are datetime type
    if not pd.api.types.is_datetime64_any_dtype(combined_df['Combined hire date']):
        combined_df['Combined hire date'] = pd.to_datetime(combined_df['Combined hire date'])
    if not pd.api.types.is_datetime64_any_dtype(combined_df['Most recent day worked']):
        combined_df['Most recent day worked'] = pd.to_datetime(combined_df['Most recent day worked'])
    
    # CRITICAL: Save a copy of the full dataset's employee information for consistent counting
    # Get distinct employees with their status info before filtering by date range
    full_employee_info = combined_df[[
        'employee_id', 'Location', 'Working Status', 'is_full_time', 
        'Combined hire date', 'Most recent day worked', 'Division'
    ]].drop_duplicates('employee_id')
    
    # Filter by date range
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
    
    # Merge attendance data back to filtered_df
    filtered_df = filtered_df.merge(
        attendance_table[['employee_id', 'date_only', 'present', 'is_present', 'visits']],
        on=['employee_id', 'date_only'],
        how='left'
    )
    
    # Fill any missing values in present and is_present columns
    filtered_df['present'] = filtered_df['present'].fillna('No')
    filtered_df['is_present'] = filtered_df['is_present'].fillna(False)
    
    # Store the full employee info for consistent denominators
    filtered_df.attrs['full_employee_info'] = full_employee_info
    
    # Calculate all analyses
    tue_thu_attendance = calculate_tue_thu_attendance_percentage(filtered_df)
    daily_counts = calculate_daily_attendance_counts(filtered_df)
    weekly_counts = calculate_weekly_attendance_counts(filtered_df)
    period_summary = calculate_period_summary(filtered_df, 
                                           pd.to_datetime(start_date) if start_date else None,
                                           pd.to_datetime(end_date) if end_date else None)
    employee_summary = create_employee_summary(filtered_df)
    
    # Calculate division attendance
    division_tue_thu = calculate_division_attendance_tue_thu(filtered_df)
    division_by_location = calculate_division_attendance_by_location(filtered_df)
    
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
        'employee_summary': employee_summary,
        'division_tue_thu': division_tue_thu,
        'division_by_location': division_by_location
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
    key_card_df, employee_df, history_df = load_data()
    
    # Clean key card data first
    key_card_df = clean_key_card_data(key_card_df)
    
    # Get the maximum date from key card data
    max_data_date = key_card_df['date_only'].max()
    print(f"\nMaximum date in key card data: {max_data_date}")
    
    # Clean employee data, passing the max_data_date
    employee_df = clean_employee_info(employee_df, max_data_date)
    
    # Merge datasets
    combined_df = merge_key_card_with_employee_info(key_card_df, employee_df, history_df)
    
    # Create attendance table to get 'present' field
    attendance_table = build_attendance_table(combined_df)
    
    # Merge attendance data back to combined_df
    combined_df = combined_df.merge(
        attendance_table[['employee_id', 'date_only', 'present', 'is_present', 'visits']],
        on=['employee_id', 'date_only'],
        how='left'
    )
    
    # Fill any missing values in is_present column
    combined_df['is_present'] = combined_df['is_present'].fillna(False)
    
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
        key_card_df, employee_df, history_df = load_data(start_date, end_date, last_n_days)
        data_load_state.text("Processing data...")
        combined_df = process_data(key_card_df, employee_df, history_df)
        
        del key_card_df
        del employee_df
        del history_df
        gc.collect()
        
        data_load_state.text("Calculating analytics...")
        analyses = calculate_analyses(combined_df, start_date, end_date)
        data_load_state.empty()
        
        min_date = combined_df['date_only'].min()
        max_date = combined_df['date_only'].max()
        st.success(f"Loaded {len(combined_df):,} records from {min_date.strftime('%d %b %Y')} to {max_date.strftime('%d %b %Y')}")
        
        # Create tabs and display data (keep existing tab code, but use analyses dict)
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "Daily Overview", 
            "Weekly Overview", 
            "Division Attendance",
            "Individual Employee Attendance",
            "Employee Details",
            "Daily Attendance Lookup"  # New tab for checking attendance by date
        ])
        
        with tab1:
            st.subheader("Daily Attendance Percentage (Tuesday-Thursday)")
            if len(analyses['tue_thu_attendance']) > 0:
                fig_daily_pct = px.line(
                    analyses['tue_thu_attendance'],
                    x='date',
                    y='percentage',
                    title='Daily Office Attendance (%) - London, Hybrid, Full-Time (Tue-Thu)',
                    labels={'percentage': 'Attendance %', 'date': 'Date'}
                )
                
                # Set hover template for more detailed date information
                fig_daily_pct.update_traces(
                    hovertemplate='%{x|%d %b %Y}<br>Attendance: %{y:.1f}%<extra></extra>'
                )
                
                # Standardize x-axis format
                fig_daily_pct.update_xaxes(
                    tickformat="%d %b %Y",
                    tickangle=-45
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
                    y=['other_count', 'london_hybrid_ft_count'],  # Order matters for stacking - other on top
                    title='Daily Attendance Count by Employee Type (Tue-Thu)',
                    labels={
                        'date': 'Date',
                        'value': 'Attendance Count',
                        'variable': 'Employee Type'
                    },
                    barmode='stack'
                )
                
                # Update trace names to be more readable
                fig_daily_counts.update_traces(
                    name='London, Hybrid, Full-Time',
                    selector=dict(name='london_hybrid_ft_count')
                )
                fig_daily_counts.update_traces(
                    name='Other Employees',
                    selector=dict(name='other_count')
                )
                
                # Set hover template for more detailed information
                fig_daily_counts.update_traces(
                    hovertemplate='%{x|%d %b %Y}<br>%{fullData.name}: %{y}<extra></extra>'
                )
                
                # Update x-axis to show day, month and year format
                fig_daily_counts.update_xaxes(
                    tickformat="%d %b %Y",
                    tickangle=-45
                )
                
                st.plotly_chart(fig_daily_counts, use_container_width=True)
            
            # Add total office attendance count line chart for London, Hybrid, Full-Time
            st.subheader("Total Office Attendance Count (London, Hybrid, Full-Time) - Tuesdays to Thursdays")
            
            # Filter for only Tue-Thu
            tue_thu_daily = analyses['daily_counts'][
                analyses['daily_counts']['day_of_week'].isin(['Tuesday', 'Wednesday', 'Thursday'])
            ]
            
            if len(tue_thu_daily) > 0:
                fig_lhft_count = px.line(
                    tue_thu_daily,
                    x='date',
                    y='london_hybrid_ft_count',
                    title='Daily London, Hybrid, Full-Time Attendance Count (Tue-Thu)',
                    labels={
                        'date': 'Date',
                        'london_hybrid_ft_count': 'Attendance Count'
                    }
                )
                
                # Set hover template for more detailed information
                fig_lhft_count.update_traces(
                    hovertemplate='%{x|%d %b %Y}<br>Attendance Count: %{y}<extra></extra>'
                )
                
                # Update x-axis format
                fig_lhft_count.update_xaxes(
                    tickformat="%d %b %Y",
                    tickangle=-45
                )
                
                st.plotly_chart(fig_lhft_count, use_container_width=True)
            
            # Daily details table
            st.subheader("Daily Attendance Details (London, Hybrid, Full-Time Analysis)")
            
            # Format the date column before display
            display_df = analyses['daily_counts'].copy()
            display_df['date'] = display_df['date'].apply(format_date)
            
            # Rename columns to be more readable
            column_mapping = {
                'date': 'Date',
                'day_of_week': 'Day of Week',
                'london_hybrid_ft_count': 'London, Hybrid, Full-Time Attendance (#)',
                'eligible_london_hybrid_ft': 'London, Hybrid, Full-Time (total #)',
                'london_hybrid_ft_percentage': 'London, Hybrid, Full-Time Attendance (%)',
                'other_count': 'Non-London, Hybrid, Full-Time Attendance (#)',
                'total_attendance': 'Total Attendance (#)'
            }
            
            display_df = display_df.rename(columns=column_mapping)
            
            # Format numbers while keeping original values
            count_columns = [
                'London, Hybrid, Full-Time Attendance (#)',
                'London, Hybrid, Full-Time (total #)',
                'Non-London, Hybrid, Full-Time Attendance (#)',
                'Total Attendance (#)'
            ]
            percentage_columns = ['London, Hybrid, Full-Time Attendance (%)']
            
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
                'London, Hybrid, Full-Time Attendance (#)',
                'London, Hybrid, Full-Time (total #)',
                'London, Hybrid, Full-Time Attendance (%)',
                'Non-London, Hybrid, Full-Time Attendance (#)',
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
                    y='london_hybrid_ft_percentage',
                    title='Weekly Office Attendance (%) - London, Hybrid, Full-Time (Tue-Thu)',
                    labels={
                        'london_hybrid_ft_percentage': 'Attendance %',
                        'week_start': 'Week Starting'
                    }
                )
                
                # Set hover template for more detailed information
                fig_weekly_pct.update_traces(
                    hovertemplate='Week of %{x|%d %b %Y}<br>Attendance: %{y:.1f}%<extra></extra>'
                )
                
                # Standardize x-axis format
                fig_weekly_pct.update_xaxes(
                    tickformat="%d %b %Y",
                    tickangle=-45
                )
                
                st.plotly_chart(fig_weekly_pct)
            
            st.subheader("Weekly Attendance Counts (Tuesday-Thursday only)")
            if len(analyses['weekly_counts']) > 0:
                fig_weekly_counts = px.bar(
                    analyses['weekly_counts'],
                    x='week_start',
                    y=['other_avg', 'london_hybrid_ft_avg'],  # Order matters for stacking - other on top
                    title='Weekly Average Attendance Count by Employee Type (Tue-Thu)',
                    labels={
                        'week_start': 'Week Starting',
                        'value': 'Average Daily Attendance',
                        'variable': 'Employee Type'
                    },
                    barmode='stack'
                )
                
                fig_weekly_counts.update_traces(
                    name='London, Hybrid, Full-Time',
                    selector=dict(name='london_hybrid_ft_avg')
                )
                fig_weekly_counts.update_traces(
                    name='Other Employees',
                    selector=dict(name='other_avg')
                )
                
                # Set hover template for more detailed information
                fig_weekly_counts.update_traces(
                    hovertemplate='Week of %{x|%d %b %Y}<br>%{fullData.name}: %{y}<extra></extra>'
                )
                
                # Update x-axis to show day, month and year format
                fig_weekly_counts.update_xaxes(
                    tickformat="%d %b %Y",
                    tickangle=-45
                )
                
                st.plotly_chart(fig_weekly_counts, use_container_width=True)
            
            # Weekly details table
            st.subheader("Weekly Attendance Details - London, Hybrid, Full-Time Focus (Tuesday-Thursday only)")
            display_cols_weekly = {
                'week_start': 'Week Starting',
                'london_hybrid_ft_avg': 'Avg. London, Hybrid, Full-Time Attendance (#)',
                'avg_eligible_london_hybrid_ft': 'Avg. London, Hybrid, Full-Time (total #)',
                'london_hybrid_ft_percentage': 'Avg. London, Hybrid, Full-Time Attendance (%)',
                'other_avg': 'Avg. Non-London, Hybrid, Full-Time Attendance (#)',
                'total_avg_attendance': 'Avg. Total Attendance (#)'
            }
            weekly_display = analyses['weekly_counts'][display_cols_weekly.keys()].rename(columns=display_cols_weekly)
            
            # Format the week_start column
            weekly_display['Week Starting'] = weekly_display['Week Starting'].apply(format_date)
            
            # Create a styled version for display
            styled_weekly = weekly_display.copy()
            
            # Format numbers
            count_columns = [
                'Avg. London, Hybrid, Full-Time Attendance (#)',
                'Avg. London, Hybrid, Full-Time (total #)',
                'Avg. Non-London, Hybrid, Full-Time Attendance (#)',
                'Avg. Total Attendance (#)'
            ]
            percentage_columns = ['Avg. London, Hybrid, Full-Time Attendance (%)']
            
            for col in count_columns:
                styled_weekly[col] = styled_weekly[col].apply(lambda x: f"{round(x):,}")
            for col in percentage_columns:
                styled_weekly[col] = styled_weekly[col].apply(lambda x: f"{x:.1f}%")
            
            # Reorder columns
            weekly_column_order = [
                'Week Starting',
                'Avg. London, Hybrid, Full-Time Attendance (#)',
                'Avg. London, Hybrid, Full-Time (total #)',
                'Avg. London, Hybrid, Full-Time Attendance (%)',
                'Avg. Non-London, Hybrid, Full-Time Attendance (#)',
                'Avg. Total Attendance (#)'
            ]
            styled_weekly = styled_weekly[weekly_column_order]
            st.dataframe(styled_weekly, hide_index=True)
        
        with tab3:
            st.subheader("Division Attendance Analysis")
            
            # Division attendance percentage chart (only Tuesdays, Wednesdays, Thursdays)
            st.subheader("1. Average Attendance (%) by Division - Tuesdays to Thursdays")
            
            if len(analyses['division_tue_thu']) > 0:
                # Sort by attendance percentage from highest to lowest
                division_tue_thu_sorted = analyses['division_tue_thu'].sort_values('attendance_percentage', ascending=False)
                
                fig_division_pct = px.bar(
                    division_tue_thu_sorted,
                    x='division',
                    y='attendance_percentage',
                    title='Division Attendance Rate (%) - London, Hybrid, Full-Time (Tue-Thu)',
                    labels={
                        'division': 'Division',
                        'attendance_percentage': 'Attendance Rate (%)'
                    },
                    color='attendance_percentage',
                    color_continuous_scale='Viridis'
                )
                
                fig_division_pct.update_layout(
                    xaxis_title='Division',
                    yaxis_title='Attendance Rate (%)'
                )
                
                st.plotly_chart(fig_division_pct, use_container_width=True)
                
                # Show the data in a table
                st.subheader("Division Attendance Details - London, Hybrid, Full-Time (Tuesdays to Thursdays)")
                styled_div_tue_thu = division_tue_thu_sorted.copy()
                styled_div_tue_thu = styled_div_tue_thu.rename(columns={
                    'division': 'Division',
                    'attendance_count': 'Average Daily Attendance (#)',
                    'eligible_count': 'Eligible Employees (#)',
                    'attendance_percentage': 'Attendance Rate (%)'
                })
                
                # Format percentages
                styled_div_tue_thu['Attendance Rate (%)'] = styled_div_tue_thu['Attendance Rate (%)'].apply(lambda x: f"{x:.1f}%")
                
                # Format counts
                styled_div_tue_thu['Average Daily Attendance (#)'] = styled_div_tue_thu['Average Daily Attendance (#)'].apply(lambda x: f"{x:.1f}")
                styled_div_tue_thu['Eligible Employees (#)'] = styled_div_tue_thu['Eligible Employees (#)'].apply(lambda x: f"{x:.1f}")
                
                st.dataframe(styled_div_tue_thu, hide_index=True)
            
            # Division attendance by location category
            st.subheader("2. Average Daily Attendance (#) by Division and Category")
            
            if len(analyses['division_by_location']) > 0:
                # Prepare the data for stacked bar chart
                division_location_data = analyses['division_by_location'].copy()
                
                # Order the divisions by total attendance
                division_location_data['total_attendance'] = (
                    division_location_data['london_hybrid_ft_count'] + 
                    division_location_data['hybrid_count'] + 
                    division_location_data['full_time_count'] + 
                    division_location_data['other_count']
                )
                division_location_sorted = division_location_data.sort_values('total_attendance', ascending=False)
                
                # Create the figure
                fig_division_location = px.bar(
                    division_location_sorted,
                    x='division',
                    y=['london_hybrid_ft_count', 'hybrid_count', 'full_time_count', 'other_count'],
                    title='Average Attendance Count by Division and Employee Category',
                    labels={
                        'division': 'Division',
                        'value': 'Average Daily Attendance',
                        'variable': 'Category'
                    },
                    barmode='stack'
                )
                
                # Update trace names
                fig_division_location.update_traces(
                    name='London, Hybrid, Full-Time',
                    selector=dict(name='london_hybrid_ft_count')
                )
                fig_division_location.update_traces(
                    name='Hybrid (non-London)',
                    selector=dict(name='hybrid_count')
                )
                fig_division_location.update_traces(
                    name='Full-Time (non-Hybrid)',
                    selector=dict(name='full_time_count')
                )
                fig_division_location.update_traces(
                    name='Other',
                    selector=dict(name='other_count')
                )
                
                # Update layout
                fig_division_location.update_layout(
                    xaxis_title='Division',
                    yaxis_title='Average Daily Attendance'
                )
                
                st.plotly_chart(fig_division_location, use_container_width=True)
                
                # Show the data in a table
                st.subheader("Division Attendance Details by Category")
                styled_div_location = division_location_sorted.copy()
                styled_div_location = styled_div_location.rename(columns={
                    'division': 'Division',
                    'london_hybrid_ft_count': 'London, Hybrid, Full-Time',
                    'hybrid_count': 'Hybrid (non-London)',
                    'full_time_count': 'Full-Time (non-Hybrid)',
                    'other_count': 'Other',
                    'total_attendance': 'Total Average Attendance'
                })
                
                # Keep only the columns we want
                styled_div_location = styled_div_location[[
                    'Division',
                    'London, Hybrid, Full-Time',
                    'Hybrid (non-London)',
                    'Full-Time (non-Hybrid)',
                    'Other',
                    'Total Average Attendance'
                ]]
                
                # Format all numeric columns with 1 decimal place
                for col in styled_div_location.columns:
                    if col != 'Division':
                        styled_div_location[col] = styled_div_location[col].apply(lambda x: f"{x:.1f}")
                
                st.dataframe(styled_div_location, hide_index=True)
        
        with tab4:
            st.subheader("Individual Employee Attendance")
            
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
            st.subheader("Employee Details")
            
            # Get the cleaned employee data
            # Load the employee data directly
            employee_df = load_employee_info("data/raw/employee_info.csv")
            
            # Get the maximum date from the combined data for proper date handling
            max_data_date = combined_df['date_only'].max() if 'date_only' in combined_df.columns else None
            
            # Clean the employee data with the max date
            employee_df = clean_employee_info(employee_df, max_data_date)
            
            # Keep only relevant columns used in calculations plus additional descriptive columns
            relevant_columns = [
                'employee_id',
                'Last name, First name',
                'Working Status',
                'Location',
                'Division',
                'Department',
                'Job Title',
                'Level',
                'Reporting to',
                'Entity',
                'Status',
                'Gender',
                'FTE',
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
                'Job Title': 'Job Title',
                'Level': 'Level',
                'Reporting to': 'Manager',
                'Entity': 'Entity',
                'Status': 'Status',
                'Gender': 'Gender',
                'FTE': 'FTE',
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
        
        with tab6:  # Daily Attendance Lookup tab
            st.subheader("London Hybrid Full-Time Employee Daily Attendance")
            st.write("Select a date to view attendance data for London-based Hybrid Full-Time employees on that day.")
            
            # Get min and max dates from the data
            min_date = combined_df['date_only'].min()
            max_date = combined_df['date_only'].max()
            
            # Date selector
            selected_date = st.date_input(
                "Select a date to view attendance:",
                value=max_date.date(),  # Default to most recent date
                min_value=min_date.date(),
                max_value=max_date.date()
            )
            
            # Get attendance data for the selected date
            if selected_date:
                date_info = pd.to_datetime(selected_date)
                day_name = date_info.strftime("%A")
                date_formatted = date_info.strftime("%d %B %Y")
                
                st.write(f"### Attendance for {day_name}, {date_formatted}")
                
                # Run attendance lookup
                with st.spinner(f"Analyzing attendance data for {date_formatted}..."):
                    try:
                        from data_analysis import get_daily_employee_attendance
                        
                        # Create a deep copy to ensure we don't modify the original 
                        debug_df = combined_df.copy(deep=True)
                        
                        # Ensure ALL columns have proper types for safe comparison
                        if 'employee_id' in debug_df.columns:
                            # Ensure employee_id is numeric - explicitly use float64 dtype
                            debug_df['employee_id'] = pd.to_numeric(debug_df['employee_id'], errors='coerce').astype('float64')
                        
                        # Handle string columns consistently
                        for col in ['Location', 'Working Status', 'Division', 'Department']:
                            if col in debug_df.columns:
                                debug_df[col] = debug_df[col].astype(str)
                        
                        # Handle date columns consistently
                        for col in ['date_only', 'Combined hire date', 'Most recent day worked']:
                            if col in debug_df.columns:
                                debug_df[col] = pd.to_datetime(debug_df[col], errors='coerce')
                        
                        # Explicitly convert date_info to pandas Timestamp
                        date_info = pd.to_datetime(date_info)
                        
                        # Add debugging output
                        print(f"Data types before get_daily_employee_attendance:")
                        print(debug_df.dtypes)
                        print(f"Selected date: {date_info}, type: {type(date_info)}")
                        
                        daily_attendance = get_daily_employee_attendance(debug_df, date_info)
                        
                        if daily_attendance.empty:
                            st.warning(f"No London-based Hybrid Full-Time employees found for {date_formatted}.")
                    except Exception as e:
                        st.error(f"Error analyzing attendance data: {str(e)}")
                        # Log detailed error for debugging
                        print(f"Detailed error in daily attendance lookup: {str(e)}")
                        
                        # Create an empty DataFrame as a fallback
                        daily_attendance = pd.DataFrame(columns=[
                            'employee_id', 'Employee Name', 'Working Status', 
                            'Location', 'Division', 'Department', 'Attended', 'Arrival Time'
                        ])
                        st.warning("Using empty dataset due to error.")
                    
                    # Only proceed if we have data and no errors
                    if not daily_attendance.empty:
                        # Summary metrics
                        total_employees = len(daily_attendance)
                        attended_count = (daily_attendance['Attended'] == 'Yes').sum()
                        attendance_rate = (attended_count / total_employees * 100) if total_employees > 0 else 0
                        
                        col1, col2, col3 = st.columns(3)
                        col1.metric("London Hybrid FT Employees", f"{total_employees}")
                        col2.metric("Employees Present", f"{attended_count}")
                        col3.metric("Attendance Rate", f"{attendance_rate:.1f}%")
                        
                        # Filter controls
                        st.write("### Employee Attendance Data")
                        
                        # Add filters for Division and Working Status
                        col1, col2 = st.columns(2)
                        with col1:
                            if 'Division' in daily_attendance.columns:
                                all_divisions = ['All Divisions'] + sorted(daily_attendance['Division'].unique().tolist())
                                selected_division = st.selectbox("Filter by Division:", all_divisions)
                        with col2:
                            if 'Working Status' in daily_attendance.columns:
                                all_statuses = ['All Statuses'] + sorted(daily_attendance['Working Status'].unique().tolist())
                                selected_status = st.selectbox("Filter by Working Status:", all_statuses)
                        
                        # Apply filters
                        filtered_data = daily_attendance.copy()
                        if selected_division != 'All Divisions':
                            filtered_data = filtered_data[filtered_data['Division'] == selected_division]
                        if selected_status != 'All Statuses':
                            filtered_data = filtered_data[filtered_data['Working Status'] == selected_status]
                        
                        # Display results
                        if filtered_data.empty:
                            st.warning("No employees match your filter criteria.")
                        else:
                            # Display columns
                            display_columns = ['Employee Name', 'Division', 'Department', 'Working Status', 'Attended', 'Arrival Time']
                            
                            # Display without styling for now due to compatibility issue
                            st.dataframe(filtered_data[display_columns], hide_index=True)
                            
                            # Add download option
                            csv = filtered_data[display_columns].to_csv(index=False)
                            st.download_button(
                                label=f"Download Attendance Data for {date_formatted}",
                                data=csv,
                                file_name=f"london_attendance_{selected_date.strftime('%Y-%m-%d')}.csv",
                                mime="text/csv"
                            )
    
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        print(f"Error details: {e}")

if __name__ == "__main__":
    main()
