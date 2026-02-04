"""Configuration loader with environment variable support."""
import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()

# Environment variable prefix
ENV_PREFIX = "COPY_TRADER_"

# Default configuration values
DEFAULTS: Dict[str, Any] = {
    "target_wallet": "",
    "starting_budget": 10000,
    "execution": {
        "check_interval": 30,
        "dry_run": True,
        "auto_execute": False,
    },
    "position_sizing": {
        "strategy": "proportional",
        "max_position_pct": 0.15,
        "min_position_pct": 0.01,
        "leverage_cap": 1.0,
    },
    "risk_management": {
        "max_daily_loss_pct": 0.10,
        "max_total_loss_pct": 0.25,
        "cooldown_after_loss": 300,
        "skip_high_risk_markets": True,
    },
    "filters": {
        "min_liquidity": 1000,
        "max_slippage": 0.05,
        "excluded_markets": [],
        "max_time_to_resolution": 2592000,
    },
    "api": {
        "max_retries": 3,
        "min_wait": 1,
        "max_wait": 30,
        "timeout": 30,
    },
    "reporting": {
        "console_output": True,
        "log_level": "INFO",
        "save_charts": True,
        "webhook_url": "",
    },
    "database": {
        "path": "trades.db",
    },
    "sheets": {
        "enabled": False,
        "sheet_id": "",
        "credentials_path": "",
    },
}

# Mapping of environment variables to config paths
ENV_MAPPING = {
    "TARGET_WALLET": "target_wallet",
    "BUDGET": "starting_budget",
    "DRY_RUN": "execution.dry_run",
    "CHECK_INTERVAL": "execution.check_interval",
    "AUTO_EXECUTE": "execution.auto_execute",
    "LOG_LEVEL": "reporting.log_level",
    "WEBHOOK_URL": "reporting.webhook_url",
    "DB_PATH": "database.path",
    "MAX_POSITION_PCT": "position_sizing.max_position_pct",
    "MIN_POSITION_PCT": "position_sizing.min_position_pct",
    "MAX_DAILY_LOSS_PCT": "risk_management.max_daily_loss_pct",
    "MAX_TOTAL_LOSS_PCT": "risk_management.max_total_loss_pct",
    "MIN_LIQUIDITY": "filters.min_liquidity",
    "API_MAX_RETRIES": "api.max_retries",
    "API_TIMEOUT": "api.timeout",
    "SHEETS_ENABLED": "sheets.enabled",
    "SHEETS_ID": "sheets.sheet_id",
    "SHEETS_CREDENTIALS": "sheets.credentials_path",
}


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """Deep merge two dictionaries."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _set_nested(config: Dict, path: str, value: Any) -> None:
    """Set a nested config value using dot notation path."""
    keys = path.split(".")
    current = config
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def _parse_env_value(value: str, current_value: Any) -> Any:
    """Parse environment variable value based on current config type."""
    if isinstance(current_value, bool):
        return value.lower() in ("true", "1", "yes", "on")
    elif isinstance(current_value, int):
        return int(value)
    elif isinstance(current_value, float):
        return float(value)
    elif isinstance(current_value, list):
        # Split comma-separated values
        return [v.strip() for v in value.split(",") if v.strip()]
    return value


def _get_nested(config: Dict, path: str, default: Any = None) -> Any:
    """Get a nested config value using dot notation path."""
    keys = path.split(".")
    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from defaults, config file, and environment variables.

    Priority (highest to lowest):
    1. Environment variables (COPY_TRADER_*)
    2. Config file (config.yaml)
    3. Default values

    Args:
        config_path: Optional path to config file. If not provided, looks for
                    config.yaml in current directory.

    Returns:
        Merged configuration dictionary
    """
    # Start with defaults
    config = _deep_merge({}, DEFAULTS)

    # Load config file if exists
    if config_path is None:
        config_path = os.getenv(f"{ENV_PREFIX}CONFIG_PATH", "config.yaml")

    config_file = Path(config_path)
    if config_file.exists():
        with open(config_file) as f:
            file_config = yaml.safe_load(f) or {}
            config = _deep_merge(config, file_config)

    # Apply environment variables
    for env_suffix, config_path_str in ENV_MAPPING.items():
        env_var = f"{ENV_PREFIX}{env_suffix}"
        env_value = os.getenv(env_var)
        if env_value is not None:
            current_value = _get_nested(config, config_path_str)
            parsed_value = _parse_env_value(env_value, current_value)
            _set_nested(config, config_path_str, parsed_value)

    return config


def get_db_path(config: Dict[str, Any]) -> str:
    """Get database path from config."""
    return config.get("database", {}).get("path", DEFAULTS["database"]["path"])


def get_webhook_url(config: Dict[str, Any]) -> Optional[str]:
    """Get webhook URL from config, returns None if empty."""
    url = config.get("reporting", {}).get("webhook_url", "")
    return url if url else None
