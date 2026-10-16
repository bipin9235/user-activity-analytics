# utils/logger.py

import logging

# ANSI color codes
RESET = "\033[0m"
YELLOW = "\033[33m"
RED = "\033[31m"

# Map log levels to colors
LOG_COLORS = {
    logging.WARNING: YELLOW,
    logging.ERROR: RED,
}

class ColoredFormatter(logging.Formatter):
    """Formatter that adds ANSI colors based on log level.

    INFO  -> normal (no color)
    WARNING -> yellow
    ERROR -> red
    """

    def format(self, record):
        color = LOG_COLORS.get(record.levelno, "")
        reset = RESET if color else ""
        # Color the entire line for warning/error; info stays normal
        record.msg = f"{color}{record.getMessage()}{reset}"
        record.args = ()  # message already formatted above
        return super().format(record)

def get_logger(name):

    try:
        log_level = dbutils.widgets.get("log_level")
    except:
        log_level = "INFO"

    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(getattr(logging, log_level.upper()))

        handler = logging.StreamHandler()

        formatter = ColoredFormatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
