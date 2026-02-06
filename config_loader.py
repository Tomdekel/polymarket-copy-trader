"""Configuration loader with environment variable support."""
import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from dotenv import load_dotenv

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
        "enable_execution_guardrails": False,
        "run_tag": "default",
        "measurement_mode": False,
        "measurement_trades": 30,
        "measurement_max_size_usd": 5.0,
        "measurement_market_filter": "all",
        "measurement_order_timeout_s": 15,
    },
    "position_sizing": {
        "strategy": "proportional",
        "max_position_pct": 0.15,
        "min_position_pct": 0.01,
        "leverage_cap": 1.0,
    },
    "risk_management": {
        "max_daily_loss_pct": 0.10,
        "max_weekly_loss_pct": 0.30,
        "max_total_loss_pct": 0.25,
        "cooldown_after_loss": 300,
        "reconciliation_epsilon": 1e-6,
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
        "execution_diagnostics_csv": "execution_diagnostics.csv",
    },
    "execution_quality": {
        "low_tier_min_liquidity": 5000,
        "low_tier_stability_seconds": 300,
        "rebalance_cooldown_seconds": 180,
        "drift_threshold_high": 0.30,
    },
    "market_making": {
        "tick_size": 0.01,
        "k_ticks": 2,
        "quote_size_usd": 10.0,
        "max_spread_pct": 0.05,
        "skew_ticks": 1.0,
        "max_hold_time_sec": 14400,
        "max_exposure_usd": 5000.0,
        "max_per_market_exposure_usd": 500.0,
        "fee_bps": 2.0,
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
    "ORDER_TYPE": "execution.order_type",
    "ENABLE_EXECUTION_GUARDRAILS": "execution.enable_execution_guardrails",
    "RUN_TAG": "execution.run_tag",
    "MEASUREMENT_MODE": "execution.measurement_mode",
    "MEASUREMENT_TRADES": "execution.measurement_trades",
    "MEASUREMENT_MAX_SIZE_USD": "execution.measurement_max_size_usd",
    "MEASUREMENT_MARKET_FILTER": "execution.measurement_market_filter",
    "LOG_LEVEL": "reporting.log_level",
    "WEBHOOK_URL": "reporting.webhook_url",
    "DB_PATH": "database.path",
    "MAX_POSITION_PCT": "position_sizing.max_position_pct",
    "MIN_POSITION_PCT": "position_sizing.min_position_pct",
    "MAX_DAILY_LOSS_PCT": "risk_management.max_daily_loss_pct",
    "MAX_TOTAL_LOSS_PCT": "risk_management.max_total_loss_pct",
    "MAX_WEEKLY_LOSS_PCT": "risk_management.max_weekly_loss_pct",
    "RECONCILIATION_EPSILON": "risk_management.reconciliation_epsilon",
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
    explicit_config_path = config_path is not None
    if not explicit_config_path:
        # Load .env for runtime execution paths that rely on implicit config loading.
        load_dotenv()
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
    """Get webhook URL from config, returns None if empty or placeholder.

    Note: Treats 'placeholder', 'disabled', and 'none' as disabled for
    compatibility with Secret Manager which doesn't allow empty values.
    """
    url = config.get("reporting", {}).get("webhook_url", "")
    # Treat certain placeholder values as disabled
    if not url or url.lower() in ("placeholder", "disabled", "none", "null"):
        return None
    return url
