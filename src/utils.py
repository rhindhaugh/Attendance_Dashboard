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

def setup_logging(log_level=None):
    """
    Setup logging configuration.
    
    Args:
        log_level: Logging level (default: None, which uses the value from config)
    
    Returns:
        Logger instance
    """
    # Import config here to avoid circular imports
    from .config import LOGS_DIR, LOG_FORMAT, DEFAULT_LOG_LEVEL
    
    # Set default log level from config if not provided
    if log_level is None:
        log_level_str = DEFAULT_LOG_LEVEL
        log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    
    # Ensure logs directory exists
    logs_dir = Path(LOGS_DIR)
    logs_dir.mkdir(exist_ok=True, parents=True)
    
    # Create a timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"attendance_{timestamp}.log"
    
    # Configure logging
    handlers = [
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
    
    # Reset any previous configuration
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Set up the new configuration
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
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

def optimize_dataframe_memory(df, logger=None):
    """
    Optimize the memory usage of a DataFrame by downcasting numeric types
    and converting object columns to categories when appropriate.
    
    Args:
        df: DataFrame to optimize
        logger: Optional logger instance
        
    Returns:
        Optimized DataFrame
    """
    if df is None or df.empty:
        return df
        
    start_mem = df.memory_usage(deep=True).sum() / 1024**2
    if logger:
        logger.debug(f"Memory usage before optimization: {start_mem:.2f} MB")
    
    # Make a copy to avoid modifying the original
    result = df.copy()
    
    # Process numeric columns
    for col in result.select_dtypes(include=['int', 'float']).columns:
        # Skip if the column has NaN values and is an integer (can't downcast with NaN)
        if result[col].isna().any() and result[col].dtype.kind in ['i', 'u']:
            continue
            
        # Get column stats for downcasting decision
        col_min = result[col].min()
        col_max = result[col].max()
        
        # Downcast based on value range
        if result[col].dtype.kind == 'i' or result[col].dtype.kind == 'u':
            if col_min >= 0:
                if col_max < 255:
                    result[col] = pd.to_numeric(result[col], downcast='unsigned')
                elif col_max < 65535:
                    result[col] = result[col].astype(np.uint16)
                elif col_max < 4294967295:
                    result[col] = result[col].astype(np.uint32)
            else:
                if col_min > -128 and col_max < 127:
                    result[col] = pd.to_numeric(result[col], downcast='signed')
                elif col_min > -32768 and col_max < 32767:
                    result[col] = result[col].astype(np.int16)
                elif col_min > -2147483648 and col_max < 2147483647:
                    result[col] = result[col].astype(np.int32)
        elif result[col].dtype.kind == 'f':
            if abs(col_min) < 1e10 and abs(col_max) < 1e10:
                result[col] = pd.to_numeric(result[col], downcast='float')
    
    # Convert object columns with low cardinality to category
    for col in result.select_dtypes(include=['object']).columns:
        # Count unique values
        num_unique = result[col].nunique()
        num_total = len(result)
        
        # If column has low cardinality (less than 50% unique values), convert to category
        if num_unique / num_total < 0.5:
            result[col] = result[col].astype('category')
    
    # Report memory savings
    end_mem = result.memory_usage(deep=True).sum() / 1024**2
    if logger:
        reduction = 100 * (start_mem - end_mem) / start_mem
        logger.debug(f"Memory usage after optimization: {end_mem:.2f} MB ({reduction:.1f}% reduction)")
    
    return result