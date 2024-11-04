"""
Handles logging setup.
"""

import logging
import os
import sys
import typing


def setup_logging() -> None:

    log_level = get_log_level()

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    fmt = "%(asctime)s :: %(levelname)s :: %(message)s"
    formatter = logging.Formatter(fmt=fmt)

    # handler for the screen
    screen_handler = logging.StreamHandler()
    screen_handler.setLevel(log_level)
    screen_handler.setFormatter(formatter)

    root_logger.addHandler(screen_handler)


def get_log_level() -> int:

    log_level_str = os.environ.get(
        "CROSSREF_LMDB_LOG_LEVEL",
        "INFO",
    ).upper()

    try:
        log_level = typing.cast(
            int,
            getattr(logging, log_level_str),
        )
    except AttributeError:
        print(f"Invalid log level: {log_level_str}")
        sys.exit(1)

    return log_level
