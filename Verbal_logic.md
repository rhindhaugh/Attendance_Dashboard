Attendance Dashboard Project: Detailed Logic Explanation
This document provides an in-depth explanation of each component in the Attendance Dashboard project, focusing on the step-by-step logic that drives the analysis.
1. data_ingestion.py
This file handles the loading of raw data from CSV files with optional date filtering.
load_key_card_data()
Purpose: Loads key card access data with configurable date filtering.
Step-by-step logic:

Defines data types for each column to ensure consistent parsing
Loads the CSV file using pandas with the specified data types
If date filtering is requested:

For last_n_days mode: calculates start date by subtracting days from end date
Converts the 'Date/time' column to datetime objects with day-first format
Applies start date filter if specified
Applies end date filter if specified


Returns the filtered DataFrame

load_employee_info()
Purpose: Loads employee information data.
Step-by-step logic:

Reads the employee CSV file into a pandas DataFrame
Logs the column names for reference
Returns the raw DataFrame without further processing

load_employment_history()
Purpose: Loads employment history data tracking status changes over time.
Step-by-step logic:

Reads the employment history CSV file into a pandas DataFrame
Converts the 'Date' column to datetime objects
Logs statistics about loaded rows and unique employment statuses
Returns the DataFrame with employment history data

calculate_default_date_range()
Purpose: Generates reasonable default date ranges for analysis.
Step-by-step logic:

Gets the current date as the end date
Calculates the start date by subtracting the specified number of days
Formats both dates as strings in 'YYYY-MM-DD' format
Returns the formatted date strings as a tuple

2. data_cleaning.py
This file transforms raw data into a consistent, usable format for analysis.
clean_key_card_data()
Purpose: Preprocesses key card data for analysis.
Step-by-step logic:

Creates an empty DataFrame to hold processed data
Extracts employee IDs from the 'User' column using regular expressions
Handles special cases where the standard regex pattern doesn't work
Logs statistics about ID extraction success
Parses the 'Date/time' column to datetime objects with day-first format
Creates a 'date_only' column by truncating timestamps to day precision
Adds 'day_of_week' column with the weekday name
Copies only needed columns from the original DataFrame
Returns the cleaned DataFrame

clean_employee_info()
Purpose: Standardizes employee data and adds derived fields.
Step-by-step logic:

Creates an empty DataFrame for processed data
Converts 'Employee #' to numeric employee_id
Logs statistics about valid employee IDs
Converts date columns (Hire Date, Original Hire Date, etc.) to datetime objects
Calculates Combined hire date by calling compute_combined_hire_date()
Determines Most recent day worked by calling compute_most_recent_day_worked()
Copies other needed columns from the original DataFrame
Standardizes Working Status values
Returns the cleaned DataFrame

compute_combined_hire_date()
Purpose: Creates a single, definitive hire date field.
Step-by-step logic:

Checks if "Original Hire Date" column exists in the input DataFrame
If it exists:

Converts both "Hire Date" and "Original Hire Date" to datetime
Takes the earlier of the two dates for each employee


If it doesn't exist:

Simply uses "Hire Date" as the combined date


Returns the DataFrame with the new "Combined hire date" column

compute_most_recent_day_worked()
Purpose: Determines each employee's last working day.
Step-by-step logic:

Initializes "Most recent day worked" column with NaT (Not a Time)
Checks if "Employment Status" column exists
If it exists:

For inactive employees: uses "Employment Status: Date" as their last day
For active employees: uses max_data_date (latest date from key card data)


If it doesn't exist:

Checks for "Status" column as an alternative
Uses "Resignation Date" for inactive employees if available
Uses max_data_date for active employees


Logs the counts of employees processed
Returns the DataFrame with the new column

create_employee_name_to_id_mapping()
Purpose: Creates a dictionary mapping employee names to their IDs.
Step-by-step logic:

Iterates through the employee DataFrame
For each employee with a valid name and ID:

Adds a mapping from name to ID in the dictionary


Logs the number of mappings created
Returns the mapping dictionary

create_employment_status_lookup()
Purpose: Creates a lookup system for tracking employment status changes over time.
Step-by-step logic:

Creates an empty dictionary to store status changes
For each row in the employment history DataFrame:

Gets the employee name, date, and status
Uses name_to_id mapping to find the corresponding employee ID
Adds the (date, status) tuple to the employee's list of status changes


Sorts each employee's status changes chronologically by date
Logs the number of employees in the lookup
Returns the lookup dictionary

is_full_time_on_date()
Purpose: Determines if an employee was Full-Time on a specific date.
Step-by-step logic:

Checks if the employee ID exists in the status lookup
If not found, returns False
Finds the most recent status change before or on the specified date
Returns True if that status was "Full-Time", False otherwise

add_full_time_indicators()
Purpose: Adds a boolean column indicating if each employee was Full-Time on each date.
Step-by-step logic:

Creates a copy of the input DataFrame
Initializes the 'is_full_time' column as False
For each row in the DataFrame:

Determines if the employee was Full-Time on that date using is_full_time_on_date()
Sets the 'is_full_time' value accordingly


Logs statistics about the number of rows marked as Full-Time
Returns the DataFrame with the 'is_full_time' column added

merge_key_card_with_employee_info()
Purpose: Combines key card data with employee information and adds full-time indicators.
Step-by-step logic:

Logs the shapes of input DataFrames
Verifies both DataFrames have 'employee_id' column
Optimizes the merge by pre-filtering employee data:

Gets unique employee IDs from key card data
Filters employee DataFrame to only include these IDs


Performs a left join on 'employee_id'
If employment history data is provided:

Creates a name-to-ID mapping from employee data
Creates an employment status lookup from history data
Adds full-time indicators to the merged DataFrame


Logs the shape of the merged result
Returns the merged DataFrame

add_time_analysis_columns()
Purpose: Adds time-based classifications for analysis.
Step-by-step logic:

Creates an empty DataFrame for results
Extracts the hour component from timestamps
Categorizes hours into meaningful time periods:

Early Morning (before 9am)
Morning (9am-12pm)
Lunch (12pm-2pm)
Afternoon (2pm-5pm)
Evening (after 5pm)


Returns the DataFrame with time analysis columns

3. data_analysis.py
This file implements the business logic for analyzing attendance patterns.
build_attendance_table()
Purpose: Creates a comprehensive table of employee attendance by date.
Step-by-step logic:

Logs diagnostic information about scans and filtering
Adds debugging for London, Hybrid, Full-Time status distribution
Extracts unique employees (IDs and names) from the dataset
Extracts unique dates from the dataset
Builds a cartesian product (every employee × every date combination)
Counts visits per employee-date combination
Merges visit counts with the cartesian product
Marks employees as "present" if they have at least one visit that day
Calculates total days attended per employee
Merges days_attended back to the main table
Sorts by employee name and date
Returns the complete attendance table

calculate_visit_counts()
Purpose: Counts total visits per employee.
Step-by-step logic:

Groups data by employee_id
Counts rows (scans) for each employee
Resets the index to convert groupby result to a standard DataFrame
Returns DataFrame with employee_id and visit counts

calculate_average_arrival_hour()
Purpose: Determines the average first entry time for each employee.
Step-by-step logic:

Sorts data by employee_id, date, and time
Groups by employee_id and date to isolate daily records
Takes the first scan of each day (earliest entry)
Extracts the hour component from these timestamps
Calculates the mean arrival hour per employee
Rounds to 2 decimal places for readability
Returns DataFrame with employee_id and mean arrival hour

calculate_daily_attendance_percentage()
Purpose: Calculates attendance percentages for each day.
Step-by-step logic:

Gets all unique dates and sorts them
For each date:

Creates filters for active employees on that date
Creates filters for London, Hybrid, Full-Time employees
Counts eligible employees meeting all criteria
Counts how many eligible employees were present
Calculates percentage: (present / eligible) * 100


Compiles results into a list of dictionaries
Returns a DataFrame with daily attendance metrics

calculate_weekly_attendance_percentage()
Purpose: Aggregates attendance by week, focusing on Tuesday-Thursday.
Step-by-step logic:

Ensures date_only is datetime
Filters for Tuesday, Wednesday, Thursday data
Adds week_commencing field (Monday of each week)
For each unique week:

Identifies eligible London, Hybrid, Full-Time employees that week
For each eligible employee, counts attended days that week
Calculates total possible days (employees × 3 days)
Computes percentage: (attended / possible) * 100


Compiles results and sorts by week_commencing
Returns DataFrame with weekly attendance percentages

calculate_attendance_by_weekday()
Purpose: Analyzes attendance patterns by day of week.
Step-by-step logic:

Defines weekday order (Monday through Friday)
Groups data by day_of_week
For each weekday, calculates:

London, Hybrid, Full-Time employee count
Other employee count (including London, Hybrid employees who aren't Full-Time)


Creates a categorical variable for proper sorting
Sorts by day_of_week
Returns DataFrame with weekday attendance counts

calculate_attendance_by_division()
Purpose: Breaks down attendance metrics by company division.
Step-by-step logic:

Gets unique divisions (excluding NaN values)
For each division:

Identifies division employees who are London, Hybrid, Full-Time
Counts attendance days within that division
Calculates total possible days based on employment periods
Computes percentage: (attendance / possible) * 100


Compiles results with division metrics
Returns DataFrame with division-level analysis

calculate_individual_attendance()
Purpose: Generates detailed metrics for each employee.
Step-by-step logic:

Logs diagnostic information
Gets date range from the data
For each unique employee:

Gets personal information (name, dates, status, full-time status)
Gets total days attended
For London, Hybrid, Full-Time employees:

Calculates core days (Tue-Thu) metrics
Analyzes arrival time patterns
Produces detailed attendance statistics




Compiles all employee metrics
Returns DataFrame with individual employee analysis

calculate_mean_arrival_time()
Purpose: Calculates average arrival time while handling outliers.
Step-by-step logic:

Checks if time series is empty
Converts times to minutes since midnight
Calculates median minutes
Identifies outliers (times >2 hours from median)
Removes outliers from calculation
Calculates mean of remaining times
Converts back to time format (HH:MM)
Returns tuple with formatted time and list of excluded outliers

create_employee_summary()
Purpose: Creates comprehensive employee attendance summary.
Step-by-step logic:

Ensures date columns are datetime
Gets full date range from data
For each employee:

Gets employment period (hire to departure)
Identifies if they're London, Hybrid, Full-Time
Counts days attended
Analyzes Tue-Thu specific metrics
Calculates arrival time statistics only for London, Hybrid, Full-Time employees
Computes attendance rate percentage only for London, Hybrid, Full-Time employees


Compiles all metrics into a summary
Sorts by London, Hybrid, Full-Time status first, then by attendance rate
Formats numeric columns for display
Returns DataFrame with employee attendance summaries

calculate_tue_thu_attendance_percentage()
Purpose: Focuses analysis on core office days (Tuesday-Thursday).
Step-by-step logic:

Filters dataset for Tuesday, Wednesday, Thursday
Gets all unique dates and sorts them
For each date:

Identifies employees employed on that date
Filters for London, Hybrid, Full-Time employees
Counts eligible employees
Counts present employees
Calculates percentage: (present / eligible) * 100


Compiles daily results
Returns DataFrame with Tue-Thu attendance percentages

calculate_daily_attendance_counts()
Purpose: Tracks daily attendance with employee type splits.
Step-by-step logic:

For each date:

Creates filters for that date, active employees, and London, Hybrid, Full-Time
Counts London, Hybrid, Full-Time employees present
Counts other employees present (including London, Hybrid who aren't Full-Time)
Gets total eligible London, Hybrid, Full-Time employees
Calculates percentage: (present / eligible) * 100
Records day of week


Adds special debugging for selected dates, now including Full-Time status checks
Compiles daily records
Returns DataFrame with daily attendance counts

calculate_weekly_attendance_counts()
Purpose: Aggregates attendance into weekly metrics.
Step-by-step logic:

Adds week_start field (Monday)
For each week:

Filters for Tue-Thu in that week
Calculates average eligible London, Hybrid, Full-Time employees each day
Gets active employees during the week
Calculates daily attendance for London, Hybrid, Full-Time employees
Calculates daily attendance for other employees
Computes weekly averages
Calculates attendance percentage


Compiles weekly records
Returns DataFrame with weekly attendance metrics

calculate_period_summary()
Purpose: Generates weekday summaries for a date range.
Step-by-step logic:

Filters dataset to the date range
For each weekday:

Creates filters for that day and for London, Hybrid, Full-Time
Calculates average London, Hybrid, Full-Time attendance
Calculates average other employee attendance
Gets eligible London, Hybrid, Full-Time count
Computes attendance percentage


Compiles weekday summaries
Returns DataFrame with weekday attendance patterns

4. dashboard.py
This file creates the Streamlit dashboard for visualizing attendance data.
load_data()
Purpose: Loads raw data with caching for performance.
Step-by-step logic:

Times the operation for performance analysis
Loads key card data with optional date filtering
Loads employee information data
Loads employment history data
Logs data loading details
Returns tuple of all three DataFrames

process_data()
Purpose: Cleans and merges data with caching.
Step-by-step logic:

Times the operation for performance analysis
Cleans key card data
Gets maximum date from key card data
Cleans employee data using the max date
Merges the datasets, including employment history for Full-Time status
Performs memory cleanup
Returns combined DataFrame

calculate_analyses()
Purpose: Runs all analyses with optional date filtering.
Step-by-step logic:

Times the operation for performance analysis
Applies date filtering if specified
Builds attendance table
Merges attendance data back to main DataFrame
Calculates various analyses:

Tuesday-Thursday attendance percentages
Daily attendance counts
Weekly attendance counts
Period summary statistics
Employee summary


Performs memory cleanup
Returns dictionary with all analysis results

main()
Purpose: Creates the Streamlit dashboard interface.
Step-by-step logic:

Sets up title and sidebar controls
Gets the most recent date in the data
Creates date range selection options:

Year to Date
Last 30 Days
Last 3 Months
Last 6 Months
2023 Full Year
2024 Full Year
Custom Date Range


Loads and processes data (including employment history) with status indicators
Creates tabs for different views:

Daily Overview
Weekly Overview
Period Summary
Employee Details
Employee Data


In Daily Overview tab:

Shows attendance percentage trend line
Displays stacked bar chart of attendance counts
Provides detailed daily metrics table


In Weekly Overview tab:

Shows weekly percentage trend line
Displays stacked bar chart of weekly averages
Provides weekly metrics table


In Period Summary tab:

Shows bar chart of weekday patterns
Provides weekday average metrics


In Employee Details tab:

Shows employee-level attendance metrics


In Employee Data tab:

Shows transformed employee dataset
Provides download option


Handles errors with appropriate messages

5. main.py
This file provides a command-line interface for batch processing.
Step-by-step logic:

Sets up argument parsing for flexible operation
Determines date range for filtering
Executes the pipeline in steps with timing:

Loading data with date filtering (including employment history)
Cleaning data and determining Full-Time status
Adding time analysis columns
Merging datasets
Running analyses
Saving results to files


Generates suffixed output files based on date range
Provides progress information and summary statistics

6. diagnose_attendance.py
This file diagnoses potential issues in attendance calculations.
Step-by-step logic:

Defines test date range (November 2024)
Tests two methods of loading the same data:

Original: Load everything and filter in memory
Optimized: Use date filtering during load


Compares row counts between methods
Processes both datasets through the pipeline (including employment history)
Compares attendance percentages from both methods
If differences exist, performs detailed comparison
Provides diagnostic output for debugging

Data Flow and Integration Between Components
The code is structured as a data processing pipeline with clear separation of concerns:

Ingestion: data_ingestion.py loads raw data with optional filtering
Cleaning: data_cleaning.py transforms and standardizes the data, adds employment status indicators
Analysis: data_analysis.py applies business logic to calculate metrics
Visualization: dashboard.py presents the results interactively

Key integration points:

The "Combined hire date" and "Most recent day worked" fields are critical for accurate attendance percentages
The "employee_id" field serves as the joining key between datasets
The "is_full_time" indicator determines if an employee was Full-Time on a given date
The London, Hybrid, Full-Time filter (Location="London UK" & Working Status="Hybrid" & is_full_time=True) defines the primary analysis group
Tuesday-Thursday is treated differently from Monday/Friday for core office day metrics
All percentage calculations use dynamic denominators that account for employment periods and status changes

Key Business Logic Elements
The most critical aspects of the business logic include:

Employee Eligibility Determination:

Only count employees between their hire date and departure date
For active employees, count through the most recent data date


London, Hybrid, Full-Time Employee Prioritization:

Primary focus on employees who are:

Based in London UK
On hybrid working arrangement
Have Full-Time employment status on the date of analysis




Dynamic Employment Status Tracking:

Uses employment history data to determine if an employee was Full-Time on any given date
Handles transitions between statuses (e.g., Temp → Full-Time → Terminated)


Tuesday-Thursday Focus:

Special emphasis on core office days (Tue-Thu)
Separate metrics from Monday/Friday patterns


Dynamic Denominators:

Percentages always use the correct eligible count for that specific date
Accounts for changing employee numbers and statuses over time


First Entry Time Analysis:

Determines arrival patterns based on first scan each day
Handles outliers to prevent skewed averages
Only calculates detailed metrics for London, Hybrid, Full-Time employees


Memory Optimization:

Strategic caching of intermediate results
Explicit garbage collection for large datasets
Selective column copying to reduce memory usage



This comprehensive approach ensures accurate attendance metrics that properly account for employee lifecycle changes and employment status transitions while providing insights into office usage patterns for the target group of London, Hybrid, Full-Time employees.