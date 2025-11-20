import logging
import os
from utils.config import config

LOG_DIR = config["logging"]["log_dir"]
LOG_FILE_NAME = config["logging"]["file"]
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR,LOG_FILE_NAME)

logging.basicConfig(
    level = config["logging"]["level"],
    format = config["logging"]["format"],
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler() if config["logging"].get("console",True) else None
    ]
)

def get_logger(name: str):
    """
    Return a logger with a consistent configuration.
    """
    return logging.getLogger(name)