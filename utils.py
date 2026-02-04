"""Utility functions for Polymarket Copy Trader."""
import json
import logging
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, Optional
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


class StructuredLogFormatter(logging.Formatter):
    """JSON formatter for Cloud Logging compatibility."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON for Cloud Logging."""
        log_entry: Dict[str, Any] = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "logging.googleapis.com/sourceLocation": {
                "file": record.pathname,
                "line": record.lineno,
                "function": record.funcName,
            },
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add any extra fields
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)

        return json.dumps(log_entry)


def is_cloud_environment() -> bool:
    """Check if running in a cloud environment (Cloud Run, GKE, etc.)."""
    # Cloud Run sets K_SERVICE, GKE sets KUBERNETES_SERVICE_HOST
    return bool(
        os.getenv("K_SERVICE")
        or os.getenv("KUBERNETES_SERVICE_HOST")
        or os.getenv("CLOUD_RUN_JOB")
    )


class InvalidWalletAddressError(ValueError):
    """Raised when a wallet address is invalid."""
    pass


def validate_wallet_address(address: str) -> str:
    """Validate an Ethereum wallet address.

    Args:
        address: The wallet address to validate

    Returns:
        The validated address (lowercased)

    Raises:
        InvalidWalletAddressError: If the address is invalid
    """
    if not address:
        raise InvalidWalletAddressError("Wallet address cannot be empty")

    # Must start with 0x
    if not address.startswith("0x"):
        raise InvalidWalletAddressError(
            f"Wallet address must start with '0x', got: {address[:10]}..."
        )

    # Must be exactly 42 characters (0x + 40 hex chars)
    if len(address) != 42:
        raise InvalidWalletAddressError(
            f"Wallet address must be 42 characters (0x + 40 hex), got {len(address)}"
        )

    # Must be valid hex after 0x
    hex_part = address[2:]
    if not re.match(r"^[0-9a-fA-F]{40}$", hex_part):
        raise InvalidWalletAddressError(
            f"Wallet address contains invalid characters: {address}"
        )

    return address.lower()

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Set up logging with environment-appropriate handler.

    In cloud environments (Cloud Run, GKE), uses JSON structured logging.
    In local environments, uses Rich console logging.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger = logging.getLogger("polymarket_copy_trader")
    logger.setLevel(level)

    # Clear existing handlers
    logger.handlers.clear()

    if is_cloud_environment():
        # Use structured JSON logging for Cloud Logging
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredLogFormatter())
    else:
        # Use Rich console logging for local development
        handler = RichHandler(console=console)
        handler.setFormatter(logging.Formatter("%(message)s"))

    logger.addHandler(handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    **extra_fields: Any,
) -> None:
    """Log a message with additional structured context fields.

    Args:
        logger: The logger to use
        level: Log level (e.g., logging.INFO)
        message: The log message
        **extra_fields: Additional fields to include in structured logs
    """
    record = logger.makeRecord(
        logger.name,
        level,
        "(unknown)",
        0,
        message,
        (),
        None,
    )
    record.extra_fields = extra_fields
    logger.handle(record)

def format_currency(value: float) -> str:
    return f"${value:,.2f}"

def format_percentage(value: float) -> str:
    return f"{value * 100:.2f}%"

def truncate_address(address: str, chars: int = 6) -> str:
    if len(address) <= chars * 2 + 3:
        return address
    return f"{address[:chars]}...{address[-chars:]}"

def format_time_ago(ts: datetime) -> str:
    now = datetime.now()
    diff = now - ts
    seconds = diff.total_seconds()
    if seconds < 60:
        return f"{int(seconds)}s ago"
    elif seconds < 3600:
        return f"{int(seconds / 60)}m ago"
    elif seconds < 86400:
        return f"{int(seconds / 3600)}h ago"
    return f"{int(seconds / 86400)}d ago"
