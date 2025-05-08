# THIS CODE IS NOT USED 

from prefect.logging import get_run_logger
import logging
from datetime import datetime
import os

def setup_file_logging(flow_name=None):
    """
    Configures a file handler for the current flow or task
    """
    logger_adapter = get_run_logger()
    logger = logger_adapter.logger  # <== akses logger asli di balik PrefectLogAdapter
    
    # Create a logs directory if it doesn't exist
    os.makedirs("/logs/flows", exist_ok=True)
    
    # Create a timestamped log file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    flow_part = f"_{flow_name}" if flow_name else ""
    log_file = f"/logs/flows/run{flow_part}_{timestamp}.log"
    
    # Avoid duplicate handlers for same file
    if any(isinstance(h, logging.FileHandler) and h.baseFilename == log_file for h in logger.handlers):
        return logger_adapter

    # Create a file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Add the file handler to the real logger
    logger.addHandler(file_handler)
    
    logger_adapter.info(f"Logging to file: {log_file}")
    
    return logger_adapter
