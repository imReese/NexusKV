"""NexusKV Unified Python Structured Logger."""

import logging
import os
import sys

_DEFAULT_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] [%(name)s] (%(filename)s:%(lineno)d) - %(message)s"
)


def get_logger(name: str = "nexuskv") -> logging.Logger:
    """Get or initialize a structured NexusKV logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(_DEFAULT_LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        level_str = os.environ.get("NEXUSKV_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_str, logging.INFO)
        logger.setLevel(level)

        logger.propagate = False
    return logger


logger = get_logger("nexuskv")
