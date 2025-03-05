"""
Configuration module for the Attendance Dashboard application.
Centralizes all configuration settings to make the application more maintainable.
"""
import os
from pathlib import Path

# Base paths
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = BASE_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
LOGS_DIR = BASE_DIR / 'logs'

# Ensure required directories exist
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Input file paths
KEY_CARD_DATA_PATH = RAW_DATA_DIR / 'key_card_access.csv'
EMPLOYEE_INFO_PATH = RAW_DATA_DIR / 'employee_info.csv'
EMPLOYMENT_HISTORY_PATH = RAW_DATA_DIR / 'employment_status_history.csv'

# Output file paths - templates that will be formatted with specific suffixes
COMBINED_DATA_TEMPLATE = str(PROCESSED_DATA_DIR / 'combined_data_{}.parquet')
ATTENDANCE_TABLE_TEMPLATE = str(PROCESSED_DATA_DIR / 'attendance_table_{}.csv')
VISIT_COUNTS_TEMPLATE = str(PROCESSED_DATA_DIR / 'visit_counts_{}.csv')
AVG_ARRIVAL_HOURS_TEMPLATE = str(PROCESSED_DATA_DIR / 'avg_arrival_hours_{}.csv')
DAYS_SUMMARY_TEMPLATE = str(PROCESSED_DATA_DIR / 'days_summary_{}.csv')

# Employee filtering criteria
LONDON_LOCATION = 'London UK'
HYBRID_WORKING_STATUS = 'Hybrid'
CORE_WEEKDAYS = ['Tuesday', 'Wednesday', 'Thursday']
CORE_WEEKDAY_INDICES = [1, 2, 3]  # 0=Monday, 1=Tuesday, etc.

# Special employee IDs that require custom handling
SPECIAL_EMPLOYEE_IDS = {
    'ROBERT_HINDHAUGH': 849,
    'BENJAMIN_MUELLER': 867
}

# Analysis settings
DEFAULT_ANALYSIS_DAYS = 365  # Default number of days to analyze
ATTENDANCE_OUTLIER_THRESHOLD = 120  # Minutes (2 hours) threshold for outlier detection

# Logging settings
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOG_LEVEL = "INFO"