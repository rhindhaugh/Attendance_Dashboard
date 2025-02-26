# Attendance Dashboard Project: Detailed Logic Explanation

This document provides an in-depth explanation of each component in the Attendance Dashboard project, focusing on the step-by-step logic that drives the analysis.

## 1. `data_ingestion.py`

This file handles the loading of raw data from CSV files with optional date filtering.

### `load_key_card_data()`

**Purpose**: Loads key card access data with configurable date filtering.

**Step-by-step logic**:
1. Defines data types for each column to ensure consistent parsing
2. Loads the CSV file using pandas with the specified data types
3. If date filtering is requested:
   - For last_n_days mode: calculates start date by subtracting days from end date
   - Converts the 'Date/time' column to datetime objects with day-first format
   - Applies start date filter if specified
   - Applies end date filter if specified
4. Returns the filtered DataFrame

### `load_employee_info()`

**Purpose**: Loads employee information data.

**Step-by-step logic**:
1. Reads the employee CSV file into a pandas DataFrame
2. Logs the column names for reference
3. Returns the raw DataFrame without further processing

### `calculate_default_date_range()`

**Purpose**: Generates reasonable default date ranges for analysis.

**Step-by-step logic**:
1. Gets the current date as the end date
2. Calculates the start date by subtracting the specified number of days
3. Formats both dates as strings in 'YYYY-MM-DD' format
4. Returns the formatted date strings as a tuple

## 2. `data_cleaning.py`

This file transforms raw data into a consistent, usable format for analysis.

### `clean_key_card_data()`

**Purpose**: Preprocesses key card data for analysis.

**Step-by-step logic**:
1. Creates an empty DataFrame to hold processed data
2. Extracts employee IDs from the 'User' column using regular expressions
3. Handles special cases where the standard regex pattern doesn't work
4. Logs statistics about ID extraction success
5. Parses the 'Date/time' column to datetime objects with day-first format
6. Creates a 'date_only' column by truncating timestamps to day precision
7. Adds 'day_of_week' column with the weekday name
8. Copies only needed columns from the original DataFrame
9. Returns the cleaned DataFrame

### `clean_employee_info()`

**Purpose**: Standardizes employee data and adds derived fields.

**Step-by-step logic**:
1. Creates an empty DataFrame for processed data
2. Converts 'Employee #' to numeric employee_id
3. Logs statistics about valid employee IDs
4. Converts date columns (Hire Date, Original Hire Date, etc.) to datetime objects
5. Calculates Combined hire date by calling compute_combined_hire_date()
6. Determines Most recent day worked by calling compute_most_recent_day_worked()
7. Copies other needed columns from the original DataFrame
8. Standardizes Working Status values
9. Returns the cleaned DataFrame

### `compute_combined_hire_date()`

**Purpose**: Creates a single, definitive hire date field.

**Step-by-step logic**:
1. Checks if "Original Hire Date" column exists in the input DataFrame
2. If it exists:
   - Converts both "Hire Date" and "Original Hire Date" to datetime
   - Takes the earlier of the two dates for each employee
3. If it doesn't exist:
   - Simply uses "Hire Date" as the combined date
4. Returns the DataFrame with the new "Combined hire date" column

### `compute_most_recent_day_worked()`

**Purpose**: Determines each employee's last working day.

**Step-by-step logic**:
1. Initializes "Most recent day worked" column with NaT (Not a Time)
2. Checks if "Employment Status" column exists
3. If it exists:
   - For inactive employees: uses "Employment Status: Date" as their last day
   - For active employees: uses max_data_date (latest date from key card data)
4. If it doesn't exist:
   - Checks for "Status" column as an alternative
   - Uses "Resignation Date" for inactive employees if available
   - Uses max_data_date for active employees
5. Logs the counts of employees processed
6. Returns the DataFrame with the new column

### `merge_key_card_with_employee_info()`

**Purpose**: Combines key card data with employee information.

**Step-by-step logic**:
1. Logs the shapes of input DataFrames
2. Verifies both DataFrames have 'employee_id' column
3. Optimizes the merge by pre-filtering employee data:
   - Gets unique employee IDs from key card data
   - Filters employee DataFrame to only include these IDs
4. Performs a left join on 'employee_id'
5. Logs the shape of the merged result
6. Returns the merged DataFrame

### `add_time_analysis_columns()`

**Purpose**: Adds time-based classifications for analysis.

**Step-by-step logic**:
1. Creates an empty DataFrame for results
2. Extracts the hour component from timestamps
3. Categorizes hours into meaningful time periods:
   - Early Morning (before 9am)
   - Morning (9am-12pm)
   - Lunch (12pm-2pm)
   - Afternoon (2pm-5pm)
   - Evening (after 5pm)
4. Returns the DataFrame with time analysis columns

## 3. `data_analysis.py`

This file implements the business logic for analyzing attendance patterns.

### `build_attendance_table()`

**Purpose**: Creates a comprehensive table of employee attendance by date.

**Step-by-step logic**:
1. Logs diagnostic information about scans and filtering
2. Extracts unique employees (IDs and names) from the dataset
3. Extracts unique dates from the dataset
4. Builds a cartesian product (every employee × every date combination)
5. Counts visits per employee-date combination
6. Merges visit counts with the cartesian product
7. Marks employees as "present" if they have at least one visit that day
8. Calculates total days attended per employee
9. Merges days_attended back to the main table
10. Sorts by employee name and date
11. Returns the complete attendance table

### `calculate_visit_counts()`

**Purpose**: Counts total visits per employee.

**Step-by-step logic**:
1. Groups data by employee_id
2. Counts rows (scans) for each employee
3. Resets the index to convert groupby result to a standard DataFrame
4. Returns DataFrame with employee_id and visit counts

### `calculate_average_arrival_hour()`

**Purpose**: Determines the average first entry time for each employee.

**Step-by-step logic**:
1. Sorts data by employee_id, date, and time
2. Groups by employee_id and date to isolate daily records
3. Takes the first scan of each day (earliest entry)
4. Extracts the hour component from these timestamps
5. Calculates the mean arrival hour per employee
6. Rounds to 2 decimal places for readability
7. Returns DataFrame with employee_id and mean arrival hour

### `calculate_daily_attendance_percentage()`

**Purpose**: Calculates attendance percentages for each day.

**Step-by-step logic**:
1. Gets all unique dates and sorts them
2. For each date:
   - Creates filters for active employees on that date
   - Creates filters for London & Hybrid employees
   - Counts eligible employees meeting both criteria
   - Counts how many eligible employees were present
   - Calculates percentage: (present / eligible) * 100
3. Compiles results into a list of dictionaries
4. Returns a DataFrame with daily attendance metrics

### `calculate_weekly_attendance_percentage()`

**Purpose**: Aggregates attendance by week, focusing on Tuesday-Thursday.

**Step-by-step logic**:
1. Ensures date_only is datetime
2. Filters for Tuesday, Wednesday, Thursday data
3. Adds week_commencing field (Monday of each week)
4. For each unique week:
   - Identifies eligible employees that week
   - For each eligible employee, counts attended days that week
   - Calculates total possible days (employees × 3 days)
   - Computes percentage: (attended / possible) * 100
5. Compiles results and sorts by week_commencing
6. Returns DataFrame with weekly attendance percentages

### `calculate_attendance_by_weekday()`

**Purpose**: Analyzes attendance patterns by day of week.

**Step-by-step logic**:
1. Defines weekday order (Monday through Friday)
2. Groups data by day_of_week
3. For each weekday, calculates:
   - London+Hybrid employee count
   - Other employee count
4. Creates a categorical variable for proper sorting
5. Sorts by day_of_week
6. Returns DataFrame with weekday attendance counts

### `calculate_attendance_by_division()`

**Purpose**: Breaks down attendance metrics by company division.

**Step-by-step logic**:
1. Gets unique divisions (excluding NaN values)
2. For each division:
   - Identifies division employees who are London+Hybrid
   - Counts attendance days within that division
   - Calculates total possible days based on employment periods
   - Computes percentage: (attendance / possible) * 100
3. Compiles results with division metrics
4. Returns DataFrame with division-level analysis

### `calculate_individual_attendance()`

**Purpose**: Generates detailed metrics for each employee.

**Step-by-step logic**:
1. Logs diagnostic information
2. Gets date range from the data
3. For each unique employee:
   - Gets personal information (name, dates, status)
   - Gets total days attended
   - For London+Hybrid employees:
     - Calculates core days (Tue-Thu) metrics
     - Analyzes arrival time patterns
     - Produces detailed attendance statistics
4. Compiles all employee metrics
5. Returns DataFrame with individual employee analysis

### `calculate_mean_arrival_time()`

**Purpose**: Calculates average arrival time while handling outliers.

**Step-by-step logic**:
1. Checks if time series is empty
2. Converts times to minutes since midnight
3. Calculates median minutes
4. Identifies outliers (times >2 hours from median)
5. Removes outliers from calculation
6. Calculates mean of remaining times
7. Converts back to time format (HH:MM)
8. Returns tuple with formatted time and list of excluded outliers

### `create_employee_summary()`

**Purpose**: Creates comprehensive employee attendance summary.

**Step-by-step logic**:
1. Ensures date columns are datetime
2. Gets full date range from data
3. For each employee:
   - Gets employment period (hire to departure)
   - Counts days attended
   - Analyzes Tue-Thu specific metrics
   - Calculates arrival time statistics
   - Computes attendance rate percentage
4. Compiles all metrics into a summary
5. Sorts by attendance rate (descending)
6. Formats numeric columns for display
7. Returns DataFrame with employee attendance summaries

### `calculate_tue_thu_attendance_percentage()`

**Purpose**: Focuses analysis on core office days (Tuesday-Thursday).

**Step-by-step logic**:
1. Filters dataset for Tuesday, Wednesday, Thursday
2. Gets all unique dates and sorts them
3. For each date:
   - Identifies employees employed on that date
   - Filters for London+Hybrid employees
   - Counts eligible employees
   - Counts present employees
   - Calculates percentage: (present / eligible) * 100
4. Compiles daily results
5. Returns DataFrame with Tue-Thu attendance percentages

### `calculate_daily_attendance_counts()`

**Purpose**: Tracks daily attendance with employee type splits.

**Step-by-step logic**:
1. For each date:
   - Creates filters for that date, active employees, and London+Hybrid
   - Counts London+Hybrid employees present
   - Counts other employees present
   - Gets total eligible London+Hybrid employees
   - Calculates percentage: (present / eligible) * 100
   - Records day of week
2. Adds special debugging for selected dates
3. Compiles daily records
4. Returns DataFrame with daily attendance counts

### `calculate_weekly_attendance_counts()`

**Purpose**: Aggregates attendance into weekly metrics.

**Step-by-step logic**:
1. Adds week_start field (Monday)
2. For each week:
   - Filters for Tue-Thu in that week
   - Calculates average eligible employees each day
   - Gets active employees during the week
   - Calculates daily attendance for London+Hybrid employees
   - Calculates daily attendance for other employees 
   - Computes weekly averages
   - Calculates attendance percentage
3. Compiles weekly records
4. Returns DataFrame with weekly attendance metrics

### `calculate_period_summary()`

**Purpose**: Generates weekday summaries for a date range.

**Step-by-step logic**:
1. Filters dataset to the date range
2. For each weekday:
   - Creates filters for that day and for London+Hybrid
   - Calculates average London+Hybrid attendance
   - Calculates average other employee attendance
   - Gets eligible London+Hybrid count
   - Computes attendance percentage
3. Compiles weekday summaries
4. Returns DataFrame with weekday attendance patterns

## 4. `dashboard.py`

This file creates the Streamlit dashboard for visualizing attendance data.

### `load_data()`

**Purpose**: Loads raw data with caching for performance.

**Step-by-step logic**:
1. Times the operation for performance analysis
2. Loads key card data with optional date filtering
3. Loads employee information data
4. Logs data loading details
5. Returns tuple of both DataFrames

### `process_data()`

**Purpose**: Cleans and merges data with caching.

**Step-by-step logic**:
1. Times the operation for performance analysis
2. Cleans key card data
3. Gets maximum date from key card data
4. Cleans employee data using the max date
5. Merges the datasets
6. Performs memory cleanup
7. Returns combined DataFrame

### `calculate_analyses()`

**Purpose**: Runs all analyses with optional date filtering.

**Step-by-step logic**:
1. Times the operation for performance analysis
2. Applies date filtering if specified
3. Builds attendance table
4. Merges attendance data back to main DataFrame
5. Calculates various analyses:
   - Tuesday-Thursday attendance percentages
   - Daily attendance counts
   - Weekly attendance counts
   - Period summary statistics
   - Employee summary
6. Performs memory cleanup
7. Returns dictionary with all analysis results

### `main()`

**Purpose**: Creates the Streamlit dashboard interface.

**Step-by-step logic**:
1. Sets up title and sidebar controls
2. Gets the most recent date in the data
3. Creates date range selection options:
   - Year to Date
   - Last 30 Days
   - Last 3 Months
   - Last 6 Months
   - 2023 Full Year
   - 2024 Full Year
   - Custom Date Range
4. Loads and processes data with status indicators
5. Creates tabs for different views:
   - Daily Overview
   - Weekly Overview
   - Period Summary
   - Employee Details
   - Employee Data
6. In Daily Overview tab:
   - Shows attendance percentage trend line
   - Displays stacked bar chart of attendance counts
   - Provides detailed daily metrics table
7. In Weekly Overview tab:
   - Shows weekly percentage trend line
   - Displays stacked bar chart of weekly averages
   - Provides weekly metrics table
8. In Period Summary tab:
   - Shows bar chart of weekday patterns
   - Provides weekday average metrics
9. In Employee Details tab:
   - Shows employee-level attendance metrics
10. In Employee Data tab:
    - Shows transformed employee dataset
    - Provides download option
11. Handles errors with appropriate messages

## 5. `main.py`

This file provides a command-line interface for batch processing.

**Step-by-step logic**:
1. Sets up argument parsing for flexible operation
2. Determines date range for filtering
3. Executes the pipeline in steps with timing:
   - Loading data with date filtering
   - Cleaning data
   - Adding time analysis columns
   - Merging datasets
   - Running analyses
   - Saving results to files
4. Generates suffixed output files based on date range
5. Provides progress information and summary statistics

## 6. `diagnose_attendance.py`

This file diagnoses potential issues in attendance calculations.

**Step-by-step logic**:
1. Defines test date range (November 2024)
2. Tests two methods of loading the same data:
   - Original: Load everything and filter in memory
   - Optimized: Use date filtering during load
3. Compares row counts between methods
4. Processes both datasets through the pipeline
5. Compares attendance percentages from both methods
6. If differences exist, performs detailed comparison
7. Provides diagnostic output for debugging

## Data Flow and Integration Between Components

The code is structured as a data processing pipeline with clear separation of concerns:

1. **Ingestion**: `data_ingestion.py` loads raw data with optional filtering
2. **Cleaning**: `data_cleaning.py` transforms and standardizes the data
3. **Analysis**: `data_analysis.py` applies business logic to calculate metrics
4. **Visualization**: `dashboard.py` presents the results interactively

Key integration points:
- The "Combined hire date" and "Most recent day worked" fields are critical for accurate attendance percentages
- The "employee_id" field serves as the joining key between datasets
- The London+Hybrid filter (Location="London UK" & Working Status="Hybrid") defines the primary analysis group
- Tuesday-Thursday is treated differently from Monday/Friday for core office day metrics
- All percentage calculations use dynamic denominators that account for employment periods

## Key Business Logic Elements

The most critical aspects of the business logic include:

1. **Employee Eligibility Determination**:
   - Only count employees between their hire date and departure date
   - For active employees, count through the most recent data date

2. **London+Hybrid Employee Prioritization**:
   - Primary focus on employees who are both:
     - Based in London UK
     - On hybrid working arrangement

3. **Tuesday-Thursday Focus**:
   - Special emphasis on core office days (Tue-Thu)
   - Separate metrics from Monday/Friday patterns

4. **Dynamic Denominators**:
   - Percentages always use the correct eligible count for that specific date
   - Accounts for changing employee numbers over time

5. **First Entry Time Analysis**:
   - Determines arrival patterns based on first scan each day
   - Handles outliers to prevent skewed averages

6. **Memory Optimization**:
   - Strategic caching of intermediate results
   - Explicit garbage collection for large datasets
   - Selective column copying to reduce memory usage

This comprehensive approach ensures accurate attendance metrics that properly account for employee lifecycle changes while providing insights into office usage patterns.