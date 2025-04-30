import logging
from pathlib import Path

def get_flow_logger(flow_name: str) -> logging.Logger:
    logs_dir = Path("/logs")
    logs_dir.mkdir(exist_ok=True)
    log_file = logs_dir / f"{flow_name}.log"

    logger = logging.getLogger(flow_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
