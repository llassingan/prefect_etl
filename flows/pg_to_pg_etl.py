from prefect import flow, task
from prefect.logging import get_run_logger
import sys
import os

# Add the utils directory to the path so we can import custom_log_handler
sys.path.append("/opt/prefect")
from utils.logger import setup_file_logging

@task
def say_hello(name):
    logger = get_run_logger()
    logger.info(f"hello {name}")

@task
def say_goodbye(name):
    logger = get_run_logger()
    logger.info(f"goodbye {name}")

@flow(name="test_flow")
def greetings(name:str):
    logger = setup_file_logging(flow_name="example1")
    logger.info("main process start")
    say_hello(name)
    say_goodbye(name)
    logger.info("main process stop")