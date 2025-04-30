from prefect import flow, task
from utils.logger import get_flow_logger
from prefect.logging import get_run_logger

@task
def say_hello(name):
    logger = get_run_logger()
    logger_local = get_flow_logger("sayhello")
    logger.info(f"hello {name}")
    logger_local.info(f"hello {name}")

@task
def say_goodbye(name):
    logger = get_run_logger()
    logger_local = get_flow_logger("saygoodbye")
    logger.info(f"goodbye {name}")
    logger_local.info(f"goodbye {name}")

@flow(name="test_flow")
def greetings(name:str):
    logger = get_run_logger()
    logger_local = get_flow_logger("main")
    logger.info("main process start")
    logger_local.info("main process start")
    say_hello(name)
    say_goodbye(name)
    logger.info("main process stop")
    logger_local.info("main process stop")