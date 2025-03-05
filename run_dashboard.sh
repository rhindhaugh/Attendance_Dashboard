#!/bin/bash

# Run attendance dashboard with configurable options
# This script provides a convenient CLI wrapper for the dashboard

# Display help information
function show_help {
    echo "Usage: $(basename "$0") [options]"
    echo
    echo "A command-line interface for the Attendance Dashboard"
    echo
    echo "Options:"
    echo "  -h, --help           Show this help message and exit"
    echo "  -a, --all-data       Process all data (no date filtering)"
    echo "  -d, --days DAYS      Process only the last N days of data (default: 365)"
    echo "  -s, --start DATE     Start date in YYYY-MM-DD format"
    echo "  -e, --end DATE       End date in YYYY-MM-DD format"
    echo "  -m, --memory-opt     Optimize memory usage (slower but uses less RAM)"
    echo "  -c, --compact        Generate compact terminal output"
    echo "  -v, --view           Launch the dashboard after processing"
    echo
    echo "Examples:"
    echo "  $(basename "$0") --days 30       # Process last 30 days of data"
    echo "  $(basename "$0") -s 2024-01-01 -e 2024-03-01  # Process specific date range"
    echo "  $(basename "$0") -a -v           # Process all data and launch dashboard"
}

# Default values
ALL_DATA=false
DAYS=365
START_DATE=""
END_DATE=""
MEMORY_OPT=false
COMPACT=false
VIEW_DASHBOARD=false

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;
        -a|--all-data)
            ALL_DATA=true
            shift
            ;;
        -d|--days)
            DAYS="$2"
            shift 2
            ;;
        -s|--start)
            START_DATE="$2"
            shift 2
            ;;
        -e|--end)
            END_DATE="$2"
            shift 2
            ;;
        -m|--memory-opt)
            MEMORY_OPT=true
            shift
            ;;
        -c|--compact)
            COMPACT=true
            shift
            ;;
        -v|--view)
            VIEW_DASHBOARD=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Build the command
CMD="python main.py"

if [ "$ALL_DATA" = true ]; then
    CMD="$CMD --all-data"
elif [ -n "$START_DATE" ] && [ -n "$END_DATE" ]; then
    CMD="$CMD --start-date $START_DATE --end-date $END_DATE"
else
    CMD="$CMD --last-days $DAYS"
fi

if [ "$MEMORY_OPT" = true ]; then
    CMD="$CMD --optimize-memory"
fi

if [ "$COMPACT" = true ]; then
    CMD="$CMD --compact-output"
fi

# Execute the command
echo "Running: $CMD"
eval "$CMD"

# Launch dashboard if requested
if [ "$VIEW_DASHBOARD" = true ]; then
    echo "Launching dashboard..."
    streamlit run src/dashboard.py
fi