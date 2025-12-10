import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from .config import config
import os


# logging.basicConfig(
#     level = config["logging"]["level"],
#     format = config["logging"]["format"],
#     handlers=[
#         logging.FileHandler(LOG_FILE),
#         logging.StreamHandler() if config["logging"].get("console",True) else None
#     ]
# )

# def get_logger(name: str):
#     """
#     Return a logger with a consistent configuration.
#     """
#     return logging.getLogger(name)

# src/utils/logger.py

def setup_logging() -> None:
    """
    Configure root logger using settings from config.yaml.
    Should be called once at app startup.
    """
    log_cfg = config.get("logging", {})
    
    level_name = log_cfg.get("level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    log_format = log_cfg.get("format", "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    log_file = log_cfg.get("file", "logs/etl.log")
    console_enabled = log_cfg.get("console", True)

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    handlers = []

    # File handler
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(logging.Formatter(log_format))
    handlers.append(file_handler)

    # Console handler
    if console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(console_handler)

    logging.basicConfig(
        level=level,
        handlers=handlers,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
