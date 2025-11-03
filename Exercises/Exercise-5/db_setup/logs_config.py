import logging
import os

def get_logger(name):
    """Configure and return a logger with neatly formatted output."""

    # Define log directory relative to this file's parent folder
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "app.log")

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # File and console handlers
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        console_handler = logging.StreamHandler()

        # Custom neat log format
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger