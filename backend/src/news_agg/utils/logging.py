"""Structured colored logging for the pipeline.

Ported from ground-news/scripts/pipeline.ts lines 59-72 (colors + log function).
"""

import logging
import sys
from datetime import datetime

GREEN = "\x1b[32m"
RED = "\x1b[31m"
YELLOW = "\x1b[33m"
DIM = "\x1b[2m"
BOLD = "\x1b[1m"
RESET = "\x1b[0m"


class PipelineFormatter(logging.Formatter):
    LEVEL_COLORS = {
        logging.DEBUG: DIM,
        logging.INFO: "",
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
        logging.CRITICAL: RED + BOLD,
    }

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.now().strftime("%H:%M:%S")
        color = self.LEVEL_COLORS.get(record.levelno, "")
        return f"{DIM}[{ts}]{RESET} {color}{record.getMessage()}{RESET}"


def get_logger(name: str = "news_agg", level: str = "info") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(PipelineFormatter())
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger
