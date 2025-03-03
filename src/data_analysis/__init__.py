# Import all functions to maintain the same API as before
import sys
import os

# Add parent directory to path to allow imports for all submodules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Now import the modules
from .attendance_table import build_attendance_table
from .attendance_counts import (
    calculate_visit_counts,
    calculate_average_arrival_hour,
    calculate_mean_arrival_time
)
from .attendance_percentage import (
    calculate_daily_attendance_percentage,
    calculate_weekly_attendance_percentage,
    calculate_tue_thu_attendance_percentage
)
from .segmentation import (
    calculate_attendance_by_weekday,
    calculate_attendance_by_division,
    calculate_division_attendance_tue_thu,
    calculate_division_attendance_by_location,
    calculate_period_summary
)
from .employee_metrics import (
    calculate_individual_attendance,
    create_employee_summary
)
from .reports import (
    calculate_daily_attendance_counts,
    calculate_weekly_attendance_counts
)