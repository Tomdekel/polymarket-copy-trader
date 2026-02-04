"""Shared test fixtures for Polymarket Copy Trader."""
import os
import tempfile
import pytest

from database import Database


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = Database(db_path)
    yield db

    # Cleanup
    db.close()
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "target_wallet": "0x1234567890abcdef1234567890abcdef12345678",
        "starting_budget": 10000,
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
        "execution": {
            "check_interval": 30,
            "dry_run": True,
            "auto_execute": False,
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
    }


@pytest.fixture
def valid_wallet_address():
    """A valid Ethereum wallet address."""
    return "0x1234567890abcdef1234567890abcdef12345678"


@pytest.fixture
def invalid_wallet_addresses():
    """List of invalid wallet addresses for testing validation."""
    return [
        "",  # Empty
        "1234567890abcdef1234567890abcdef12345678",  # Missing 0x
        "0x12345",  # Too short
        "0x1234567890abcdef1234567890abcdef12345678extra",  # Too long
        "0xGGGG567890abcdef1234567890abcdef12345678",  # Invalid hex characters
        "0X1234567890abcdef1234567890abcdef12345678",  # Uppercase 0X (should still work after lowercase)
    ]
