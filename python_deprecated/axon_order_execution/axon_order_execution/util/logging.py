"""
Colored logging utilities with timestamp support and daily rotation.
"""

import logging
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_initialized = False


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and timestamps."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        timestamp = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_message = super().format(record)
        colored_level = f"{color}{self.BOLD}{record.levelname}{self.RESET}"
        return f"{self.RESET}[{timestamp}] {colored_level} - {log_message}"


class PlainFormatter(logging.Formatter):
    """Plain formatter without colors for file output."""

    def format(self, record):
        timestamp = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_message = super().format(record)
        return f"[{timestamp}] {record.levelname} - {log_message}"


def init_logging(
    level: int = logging.INFO,
    format_string: str = "%(name)s - %(message)s",
    log_file: str = "logs/oms.log",
    file_level: int | None = None,
    console_output: bool = False,
    rotate_daily: bool = True,
    backup_count: int = 30,
) -> None:
    """Initialize logging for the application. Call once at startup.

    Configures the root logger with colored console output and file rotation.
    All modules using `get_logger(__name__)` will inherit this configuration.

    Args:
        level: Logging level for console output.
        format_string: Format string for log messages.
        log_file: Path to log file. Set to None to disable file logging.
        file_level: Logging level for file output. Defaults to same as level.
        console_output: Whether to output to console with colors.
        rotate_daily: Whether to rotate log files daily.
        backup_count: Number of backup files to keep (days).
    """
    global _initialized
    if _initialized:
        return

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()

    # Create console handler with colored output
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        colored_formatter = ColoredFormatter(format_string)
        console_handler.setFormatter(colored_formatter)
        root_logger.addHandler(console_handler)

    # Create file handler with plain output
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        if rotate_daily:
            file_name = log_path.stem
            file_ext = log_path.suffix

            file_handler = TimedRotatingFileHandler(
                filename=log_file,
                when="midnight",
                interval=1,
                backupCount=backup_count,
                encoding="utf-8",
            )

            def custom_namer(default_name):
                base_filename = str(log_path.parent / file_name)
                date_str = default_name.split(".")[-1]
                date_str = date_str.replace("-", "_")
                return f"{base_filename}.{date_str}{file_ext}"

            file_handler.namer = custom_namer
        else:
            file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")

        file_handler.setLevel(file_level if file_level is not None else level)
        plain_formatter = PlainFormatter(format_string)
        file_handler.setFormatter(plain_formatter)
        root_logger.addHandler(file_handler)

    _initialized = True


def get_logger(name: str) -> logging.Logger:
    """Get a logger by name. Use in each module.

    Args:
        name: Logger name, typically __name__.

    Returns:
        Logger instance that inherits from root logger configuration.
    """
    return logging.getLogger(name)
