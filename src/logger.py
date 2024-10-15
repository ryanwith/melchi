import logging
import sys

def setup_logger(name, level=logging.INFO):
    """Set up a logger with console output."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create console handler and set level
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to ch
    ch.setFormatter(formatter)

    # Add ch to logger
    logger.addHandler(ch)

    return logger

# Create a default logger
default_logger = setup_logger('melchi_data_movement')