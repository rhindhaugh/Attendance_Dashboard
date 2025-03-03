import logging
import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

# Configure basic logging if not already configured
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def setup_logging(log_level=logging.INFO):
    """
    Setup logging configuration.
    
    Args:
        log_level: Logging level (default: INFO)
    
    Returns:
        Logger instance
    """
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Create a timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"attendance_{timestamp}.log"
    
    # Configure logging
    handlers = [
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers
    )
    
    # Create and return logger
    logger = logging.getLogger("attendance_dashboard")
    
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

def safe_data_frame_operation(operation, error_message, logger, *args, **kwargs):
    """
    Execute a DataFrame operation with proper error handling.
    
    Args:
        operation: Function to execute
        error_message: Error message prefix
        logger: Logger instance
        *args, **kwargs: Arguments to pass to the operation
        
    Returns:
        Result of the operation or None on error
    """
    try:
        return operation(*args, **kwargs)
    except Exception as e:
        logger.error(f"{error_message}: {str(e)}")
        return None

def handle_empty_dataframe(df, operation_name, logger):
    """
    Check if DataFrame is empty and log a warning if it is.
    
    Args:
        df: DataFrame to check
        operation_name: Name of the operation that would be performed
        logger: Logger instance
        
    Returns:
        True if DataFrame is empty, False otherwise
    """
    if df is None or df.empty:
        logger.warning(f"Empty DataFrame provided for {operation_name}. Operation skipped.")
        return True
    return False

def validate_columns(df, required_columns, operation_name, logger):
    """
    Validate that DataFrame contains all required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present
        operation_name: Name of the operation that requires these columns
        logger: Logger instance
        
    Returns:
        True if validation passes, False otherwise
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing required columns for {operation_name}: {missing_columns}")
        return False
    return True