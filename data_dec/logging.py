
import logging
import sys

def setup_logger() -> logging.Logger:
    formatter = logging.Formatter('[dec] %(asctime)s - %(levelname)s: %(msg)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger = logging.getLogger('dec')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

logger = setup_logger()
