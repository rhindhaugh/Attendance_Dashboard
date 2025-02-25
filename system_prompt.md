--------------------------------------------------------------------------------------------------------------------------------------------------------
THIS IS A INITIAL PROMPT FOR AN LLM TO GET THEM UP TO SPEED ON THE PROJECT. ALSO USEFUL FOR CURSOR AI AS CONTEXT
--------------------------------------------------------------------------------------------------------------------------------------------------------

SYSTEM PROMPT / SUMMARY
Project Context
You are assisting with a project to analyze office attendance data. The user has two CSV data sources:

Key card CSV (originally 300,000+ rows) with columns for date/time scans and user identifiers. Over the conversation, it evolved to have columns like employee_id, timestamp, and access_point.
Employee info CSV (1,200+ rows), containing detailed information about each employee (Employee #, status, hire date, department, etc.), which gets renamed to employee_id for merging.
The goal is to clean, merge, and analyze these datasets. The user also wants a Streamlit dashboard to visualize attendance trends (visit frequency, arrival times, etc.).

Directory Structure & Python Setup

A recommended repo layout was established, including data/raw/, data/processed/, src/ scripts (data_ingestion.py, data_cleaning.py, data_analysis.py, dashboard.py), plus optional notebooks/ and tests/.
The user created a virtual environment (with venv) and a requirements.txt specifying libraries (pandas, numpy, streamlit, plotly, etc.).
Data Handling Steps

Load CSVs: data_ingestion.py has functions like load_key_card_data(...) and load_employee_info(...).
Cleaning:
Key card data needed to parse date/time columns (day-first) and extract numeric IDs from strings (like "761 Clark Hemmings, Andre"). Rows without a valid numeric ID (e.g., cleaners) are dropped.
Employee info needed "Employee #" renamed to employee_id.
Merging: A function merges both DataFrames on employee_id.
Analysis: Simple functions in data_analysis.py calculate attendance stats (visit counts, average arrival hour).
Dashboard: A basic Streamlit app (dashboard.py) loads the data, cleans it, merges it, and shows tables/charts.
Key Issues Encountered & Their Solutions

ModuleNotFoundError: The user hadn't installed pandas in the correct environment or had a malformed requirements.txt. Solution: ensure the correct environment is activated and the requirements.txt is correct.
Column mismatch (KeyError: 'Date/time'): The key card CSV actually had a column named "timestamp" instead of "Date/time". The solution was to match code references to the actual headers (or rename the CSV header).
Small Test File Instead of Real 100k Rows: The user saw only 100 rows and IDs like 1, 2, 3..., instead of 3-digit IDs. It turned out the CSV might be a test snippet, not the real data. The solution is to confirm the real file (with ~100k rows) is placed in data/raw/ and that the correct path is used.
NaNs after Merge: Some employee IDs in the key card data do not appear in the employee info CSV. Rows with missing matches lead to NaN values for employee info columns. If desired, an inner join or filtering out invalid IDs can remove those unmatched rows.
Important Instruction for the LLM
In any further conversation about this project:

Ask clarifying questions or request to see code/config files whenever something is ambiguous, especially if column names, file paths, or data shapes aren't confirmed.
Do not rush to provide partial solutions based on assumptions—always confirm details if they are missing or unclear.
End of Summary

The key card data is only for the London office. However, all employees might scan in (e.g. employees from other offices might be visiting London).
Further, 

ADDENDUM: ADDITIONAL PROJECT CONTEXT
Employee Data Transformations
The project now incorporates several derived fields from employee data that are critical for accurate attendance analysis:

Status v2: Distinguishes between active employees, inactive employees, and upcoming hires
Combined hire date: Uses Original Hire Date when available, otherwise Hire Date
Most recent day worked: For inactive employees, uses Employment Status Date; for active employees, uses today's date
Tenure: Calculates employment duration based on hire and departure dates
Location group: Maps detailed office locations to broader categories (London, Kent, US, Other)
Ops or non-ops: Binary categorization of divisions
Year flags (2014-2025): Tracks which employees were active in each calendar year

Attendance Analysis Requirements
The attendance analysis has specific business requirements:

Focus on London+Hybrid employees: Primary attendance metrics target employees who are both:

London-based (Location = "London UK")
Hybrid workers (Working Status = "Hybrid")


Dynamic denominators for percentages: When calculating attendance percentages:

Must account for changing employee counts over time
Only include employees who were actually employed on a given date (between Combined hire date and Most recent day worked)
Different calculations needed for different time periods (weekday splits, division-specific metrics)


Dashboard structure: The dashboard should include:

Overview: Daily/weekly attendance percentages, weekday counts, division breakdowns
Daily view: Current-day attendance with London+Hybrid vs Other categories
Individual metrics: Per-employee stats with separate tracking for Mon/Fri vs Tues-Thurs attendance


Data segmentation needs: Analysis requires:

Monday/Friday vs Tuesday-Thursday comparison
Operation vs non-operations divisions
Location-based grouping
Tenure-based analysis



Technical Implementation Notes

Enhanced clean_employee_info() function in data_cleaning.py now handles all employee data transformations
New analysis functions in data_analysis.py implement the business logic for attendance calculations
Dashboard (dashboard.py) needs to display metrics with both raw numbers and percentage views where appropriate
All percentage calculations must use the appropriate dynamic denominator methods

The focus of the project has shifted from basic attendance tracking to sophisticated workforce analytics that account for employee lifecycle changes and provide accurate metrics for hybrid work attendance patterns.

ADDENDUM: PROJECT EVOLUTION AND IMPLEMENTATION DETAILS
Key Data Processing Insights
Key Card Data Cleaning

Custom ID Extraction: The original regex for extracting employee IDs wasn't capturing all formats. Implemented improved extraction with custom mappings for special cases like "Payne, James" (ID 735) and "Arorra, Aakash" (ID 378).
Data Type Consistency: Must ensure employee_id is consistently treated as a numeric type throughout processing to avoid merge issues.
Entrance Location Analysis: The "Where" column provides valuable information about which entrances employees use, potentially useful for building usage analysis.

Employee Information Processing

Employment Period Calculation: Critical to accurately determine when employees were active for correct attendance percentage calculations:

Use Combined hire date 
For departed employees, use Most recent day worked from Employment Status Date
Only count employees in denominators when they were actually employed


Employee Status Filtering: Need to distinguish active, inactive, and future employees when calculating metrics

Data Merging Challenges

ID Format Mismatches: Employee IDs sometimes have different types/formats between datasets (string vs float)
Analysis Implementation
Attendance Calculations

Present/Absent Logic: An employee is considered "present" if they have at least one scan on a given day
London+Hybrid Employees: Primary focus group defined by Location == "London UK" & Working Status == "Hybrid"
Dynamic Denominators: When calculating percentages, only include employees who were actually employed on a specific date (between Combined hire date and Most recent day worked)
Tue-Thu vs Mon/Fri Analysis: Created separate metrics for Tuesday-Thursday (core office days) vs Monday/Friday
First Entry Time Analysis: Extract earliest scan time per day per employee for arrival time analysis

Period-Based Metrics

Daily Metrics: Track attendance for each calendar day, with separate counts for London+Hybrid and other employees
Weekly Aggregation: Group data by week (starting Mondays) for trend analysis
Custom Period Summaries: Calculate metrics for specific timeframes (last 30 days, last 90 days)
Weekday Patterns: Analyze attendance by day of week to reveal weekly patterns

Dashboard Enhancements
Interface Improvements

Multi-tab Design: Organized content into logical sections (Overview Summary, Daily, Weekly, and Employee Details)
Date Range Filtering: Added sidebar date selectors that filter all visualizations and tables simultaneously
Quick Date Presets: Added buttons for common time periods (last 30 days, last 90 days)

Visualization Types

Line Charts: For attendance percentage trends over time
Stacked Bar Charts: For comparing London+Hybrid vs other employee attendance
Dual-Axis Charts: Combining raw counts (bars) with percentages (line) for context
Detailed Tables: For specific metrics and individual employee analytics
Interactive Elements: Allow users to explore data through filters and toggles

Performance Considerations

Calculated Metrics: Some analyses (like employee summary) should be recomputed when date filters change
Debug Information: Added detailed validation output to verify data processing correctness
Error Handling: Added appropriate error messages when data is unavailable for selected date ranges

Technical Implementation Lessons
Data Processing

Workflow Organization: Clear separation between loading, cleaning, merging, and analysis functions
Function Naming: Descriptive names that indicate what each analysis does (e.g., calculate_daily_attendance_percentage)
Consistent DataFrame Handling: Copy DataFrames before modifying them to avoid unexpected changes
Date Handling: Consistent use of pandas datetime objects throughout the pipeline

Code Structure

Manual Corrections: Added capability to handle edge cases and data anomalies
Debugging Output: Strategic print statements to validate data transformations
Error Handling: Added try/except blocks to provide meaningful error messages
Function Documentation: Clear docstrings explaining purpose, parameters, and return values

Streamlit Dashboard

State Management: Preserving selected date ranges across tab changes
Consistent Formatting: Using the same column naming conventions in display tables
Component Organization: Logical grouping of related visualizations and controls
User Experience: Informative labels, titles, and tooltips for all visualizations

Future Enhancement Opportunities

Division Analysis: Deeper comparison of attendance patterns across different divisions
Building Usage Patterns: Analysis of entrance usage and flow through the building
Tenure-based Segmentation: Compare attendance patterns by employee tenure
API Interface: Created FastAPI endpoints for programmatic access to attendance data
Advanced Time Period Analysis: Seasonality detection, holiday impact, and trends over time
Mobile Optimization: Ensuring dashboard works well on various screen sizes

This implementation offers a comprehensive view of office attendance patterns while accounting for the complexities of hybrid work arrangements and changing employee lifecycles.