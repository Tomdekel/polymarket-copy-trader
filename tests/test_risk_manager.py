"""Tests for risk manager module."""
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch

from risk_manager import RiskManager


class TestRiskManager:
    """Test RiskManager class."""

    def test_init_with_config(self, sample_config):
        """Test initialization with configuration."""
        rm = RiskManager(sample_config)

        assert rm.max_daily_loss == 0.10
        assert rm.max_total_loss == 0.25
        assert rm.cooldown_seconds == 300
        assert rm.min_liquidity == 1000

    def test_check_risk_allows_trade_normally(self, sample_config):
        """Test that trading is allowed under normal conditions."""
        rm = RiskManager(sample_config)
        rm.set_starting_budget(10000)

        result = rm.check_risk(current_pnl=100)  # Positive PnL

        assert result["allow_trade"] is True
        assert result["reason"] is None

    def test_check_risk_halts_on_daily_loss(self, sample_config):
        """Test trading halt when daily loss limit is hit."""
        rm = RiskManager(sample_config)
        rm.set_starting_budget(10000)
        rm.daily_pnl = -1100  # More than 10% daily loss

        result = rm.check_risk(current_pnl=-1100)

        assert result["allow_trade"] is False
        assert "Daily loss limit" in result["reason"]

    def test_check_risk_halts_on_total_loss(self, sample_config):
        """Test trading halt when total loss limit is hit."""
        rm = RiskManager(sample_config)
        rm.set_starting_budget(10000)

        result = rm.check_risk(current_pnl=-2600)  # More than 25% total loss

        assert result["allow_trade"] is False
        assert "Total loss limit" in result["reason"]

    def test_check_risk_cooldown_after_loss(self, sample_config):
        """Test trading halt during cooldown period."""
        rm = RiskManager(sample_config)
        rm.set_starting_budget(10000)

        # Record a recent loss
        rm.record_loss()

        result = rm.check_risk(current_pnl=0)

        assert result["allow_trade"] is False
        assert "Cooldown" in result["reason"]

    def test_cooldown_expires(self, sample_config):
        """Test that cooldown expires after the specified time."""
        rm = RiskManager(sample_config)
        rm.set_starting_budget(10000)

        # Set loss time to 6 minutes ago (more than 5 minute cooldown)
        rm.last_loss_time = datetime.now() - timedelta(seconds=360)

        result = rm.check_risk(current_pnl=0)

        assert result["allow_trade"] is True

    def test_daily_pnl_resets(self, sample_config):
        """Test that daily PnL resets after 24 hours."""
        rm = RiskManager(sample_config)
        rm.set_starting_budget(10000)
        rm.daily_pnl = -500

        # Set last reset to more than 24 hours ago
        rm.last_reset = datetime.now() - timedelta(days=2)

        rm.check_risk(current_pnl=0)

        assert rm.daily_pnl == 0

    def test_check_risk_without_starting_budget(self, sample_config):
        """Test that trading is allowed when starting budget not set."""
        rm = RiskManager(sample_config)
        # Don't set starting budget

        result = rm.check_risk(current_pnl=-10000)

        assert result["allow_trade"] is True


class TestRiskManagerMarketFilters:
    """Test market filtering in RiskManager."""

    def test_can_trade_market_sufficient_liquidity(self, sample_config):
        """Test market with sufficient liquidity."""
        rm = RiskManager(sample_config)

        assert rm.can_trade_market(5000, "test-market") is True

    def test_can_trade_market_insufficient_liquidity(self, sample_config):
        """Test market with insufficient liquidity."""
        rm = RiskManager(sample_config)

        assert rm.can_trade_market(500, "test-market") is False

    def test_can_trade_market_exact_minimum(self, sample_config):
        """Test market at exact minimum liquidity."""
        rm = RiskManager(sample_config)

        assert rm.can_trade_market(1000, "test-market") is True


class TestRiskManagerEdgeCases:
    """Test edge cases in risk management."""

    def test_positive_pnl_always_allowed(self, sample_config):
        """Test that positive PnL never triggers loss limits."""
        rm = RiskManager(sample_config)
        rm.set_starting_budget(100)  # Small budget

        result = rm.check_risk(current_pnl=10000)  # Huge profit

        assert result["allow_trade"] is True

    def test_zero_budget(self, sample_config):
        """Test handling of zero starting budget."""
        rm = RiskManager(sample_config)
        rm.set_starting_budget(0)

        # Should not crash on division by zero
        result = rm.check_risk(current_pnl=-100)

        # With zero budget, any loss percentage would be infinite
        # Implementation should handle this gracefully
        assert result is not None
