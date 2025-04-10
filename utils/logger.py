import logging
import time
from functools import wraps

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

def track_performance(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__name__)
        start_time = time.time()
        logger.info(f"Starting: {func.__name__}")
        result = func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Completed: {func.__name__} in {end_time - start_time:.2f} seconds")
        return result
    return wrapper
