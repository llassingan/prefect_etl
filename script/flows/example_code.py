from prefect import flow, task
from prefect.logging import get_run_logger
import sys
import os

# Add the utils directory to the path so we can import custom_log_handler
sys.path.append("/opt/prefect")
from utils.logger import setup_file_logging

@task
def extract_data():
    logger = get_run_logger()
    logger.info("Extracting data from source")
    # Your extraction logic here
    return {"data": "sample data"}

@task
def transform_data(data):
    logger = get_run_logger()
    logger.info(f"Transforming data: {data}")
    # Your transformation logic here
    return {"transformed": data["data"].upper()}

@task
def load_data(data):
    logger = get_run_logger()
    logger.info(f"Loading data: {data}")
    # Your loading logic here
    logger.info("Data loaded successfully")
    return True

@flow(name="example_logging_flow")
def example_flow(name="default"):
    """Example flow with custom file logging"""
    # Setup custom file logging for this flow
    logger = setup_file_logging(flow_name="example")
    
    logger.info(f"Starting flow for {name}")
    
    try:
        data = extract_data()
        transformed_data = transform_data(data)
        result = load_data(transformed_data)
        
        logger.info("Flow completed successfully")
        return result
    except Exception as e:
        logger.error(f"Flow failed with error: {e}")
        raise

if __name__ == "__main__":
    example_flow()