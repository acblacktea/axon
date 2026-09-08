"""
Colored logging utilities with timestamp support
"""
import logging
import os
from datetime import datetime
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and timestamps"""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'

    def format(self, record):
        # Get color for log level
        color = self.COLORS.get(record.levelname, self.RESET)

        # Format timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Format the log message
        log_message = super().format(record)

        # Add color to level name
        colored_level = f"{color}{self.BOLD}{record.levelname}{self.RESET}"

        # Build final message with timestamp and color
        return f"{self.RESET}[{timestamp}] {colored_level} - {log_message}"


class PlainFormatter(logging.Formatter):
    """Plain formatter without colors for file output"""

    def format(self, record):
        # Format timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Format the log message
        log_message = super().format(record)

        # Build final message with timestamp (no colors)
        return f"[{timestamp}] {record.levelname} - {log_message}"


def setup_colored_logger(
    name: str = None,
    level: int = logging.INFO,
    format_string: str = "%(name)s - %(message)s",
    log_file: str = "logs/mds.log",
    file_level: int = None,
    rotate_daily: bool = True,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    # Create file handler with plain output
    if log_file:
        # Create logs directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        if rotate_daily:
            # Use TimedRotatingFileHandler for daily rotation
            # Extract base name and extension
            file_name = log_path.stem  # e.g., "mds"
            file_ext = log_path.suffix  # e.g., ".log"

            # Create handler that rotates at midnight
            file_handler = TimedRotatingFileHandler(
                filename=log_file,
                when='midnight',
                interval=1,
                backupCount=30,  # Keep 30 days of logs
                encoding='utf-8'
            )

            # Custom namer to format the rotated files as: mds.2026_01_13.log
            def custom_namer(default_name):
                # default_name will be like: logs/mds.log.2026-01-13
                base_filename = str(log_path.parent / file_name)
                # Extract the date part from default_name
                date_str = default_name.split('.')[-1]  # Get "2026-01-13"
                # Replace hyphens with underscores
                date_str = date_str.replace('-', '_')
                return f"{base_filename}.{date_str}{file_ext}"

            file_handler.namer = custom_namer
        else:
            # Use regular FileHandler without rotation
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')

        file_handler.setLevel(file_level if file_level is not None else level)
        plain_formatter = PlainFormatter(format_string)
        file_handler.setFormatter(plain_formatter)
        logger.addHandler(file_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger
