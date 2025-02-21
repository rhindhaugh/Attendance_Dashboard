--------------------------------------------------------------------------------------------------------------------------------------------------------
THIS IS A INITIAL PROMPT FOR AN LLM TO GET THEM UP TO SPEED ON THE PROJECT. ALSO USEFUL FOR CURSOR AI AS CONTEXT
--------------------------------------------------------------------------------------------------------------------------------------------------------

SYSTEM PROMPT / SUMMARY
Project Context
You are assisting with a project to analyze office attendance data. The user has two CSV data sources:

Key card CSV (originally 100,000+ rows) with columns for date/time scans and user identifiers. Over the conversation, it evolved to have columns like employee_id, timestamp, and access_point.
Employee info CSV (800+ rows), containing detailed information about each employee (Employee #, status, hire date, department, etc.), which gets renamed to employee_id for merging.
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

ModuleNotFoundError: The user hadn’t installed pandas in the correct environment or had a malformed requirements.txt. Solution: ensure the correct environment is activated and the requirements.txt is correct.
Column mismatch (KeyError: 'Date/time'): The key card CSV actually had a column named "timestamp" instead of "Date/time". The solution was to match code references to the actual headers (or rename the CSV header).
Small Test File Instead of Real 100k Rows: The user saw only 100 rows and IDs like 1, 2, 3..., instead of 3-digit IDs. It turned out the CSV might be a test snippet, not the real data. The solution is to confirm the real file (with ~100k rows) is placed in data/raw/ and that the correct path is used.
NaNs after Merge: Some employee IDs in the key card data do not appear in the employee info CSV. Rows with missing matches lead to NaN values for employee info columns. If desired, an inner join or filtering out invalid IDs can remove those unmatched rows.
Important Instruction for the LLM
In any further conversation about this project:

Ask clarifying questions or request to see code/config files whenever something is ambiguous, especially if column names, file paths, or data shapes aren’t confirmed.
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