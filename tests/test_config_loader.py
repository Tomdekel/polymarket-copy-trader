"""Tests for config loader module."""
import os
import pytest
import tempfile
import yaml
from unittest.mock import patch

from config_loader import load_config, get_db_path, get_webhook_url, DEFAULTS


class TestLoadConfig:
    """Test config loading functionality."""

    def test_load_defaults_when_no_file(self):
        """Test that defaults are loaded when no config file exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = load_config(os.path.join(tmpdir, "nonexistent.yaml"))

            assert config["starting_budget"] == DEFAULTS["starting_budget"]
            assert config["execution"]["dry_run"] is True

    def test_load_from_yaml_file(self):
        """Test loading config from YAML file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"starting_budget": 5000}, f)
            f.flush()

            config = load_config(f.name)

            assert config["starting_budget"] == 5000
            # Defaults should still be present
            assert "execution" in config

            os.unlink(f.name)

    def test_env_vars_override_file(self):
        """Test that environment variables override file config."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"starting_budget": 5000}, f)
            f.flush()

            with patch.dict(os.environ, {"COPY_TRADER_BUDGET": "8000"}):
                config = load_config(f.name)

                assert config["starting_budget"] == 8000

            os.unlink(f.name)

    def test_env_var_boolean_parsing(self):
        """Test parsing of boolean environment variables."""
        with patch.dict(os.environ, {"COPY_TRADER_DRY_RUN": "false"}):
            config = load_config()

            assert config["execution"]["dry_run"] is False

        with patch.dict(os.environ, {"COPY_TRADER_DRY_RUN": "true"}):
            config = load_config()

            assert config["execution"]["dry_run"] is True

        with patch.dict(os.environ, {"COPY_TRADER_DRY_RUN": "1"}):
            config = load_config()

            assert config["execution"]["dry_run"] is True

    def test_env_var_integer_parsing(self):
        """Test parsing of integer environment variables."""
        with patch.dict(os.environ, {"COPY_TRADER_CHECK_INTERVAL": "60"}):
            config = load_config()

            assert config["execution"]["check_interval"] == 60

    def test_env_var_float_parsing(self):
        """Test parsing of float environment variables."""
        with patch.dict(os.environ, {"COPY_TRADER_MAX_POSITION_PCT": "0.20"}):
            config = load_config()

            assert config["position_sizing"]["max_position_pct"] == 0.20

    def test_nested_config_merge(self):
        """Test that nested configs are properly merged."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({
                "execution": {
                    "check_interval": 45,
                    # dry_run should remain from defaults
                }
            }, f)
            f.flush()

            config = load_config(f.name)

            assert config["execution"]["check_interval"] == 45
            assert config["execution"]["dry_run"] is True  # Default preserved

            os.unlink(f.name)


class TestGetDbPath:
    """Test database path retrieval."""

    def test_get_db_path_from_config(self):
        """Test getting DB path from config."""
        config = {"database": {"path": "/custom/path/db.sqlite"}}
        assert get_db_path(config) == "/custom/path/db.sqlite"

    def test_get_db_path_default(self):
        """Test getting default DB path."""
        config = {}
        assert get_db_path(config) == "trades.db"


class TestGetWebhookUrl:
    """Test webhook URL retrieval."""

    def test_get_webhook_url_from_config(self):
        """Test getting webhook URL from config."""
        config = {"reporting": {"webhook_url": "https://discord.com/api/webhooks/123"}}
        assert get_webhook_url(config) == "https://discord.com/api/webhooks/123"

    def test_get_webhook_url_empty_returns_none(self):
        """Test that empty webhook URL returns None."""
        config = {"reporting": {"webhook_url": ""}}
        assert get_webhook_url(config) is None

    def test_get_webhook_url_missing_returns_none(self):
        """Test that missing webhook URL returns None."""
        config = {}
        assert get_webhook_url(config) is None
