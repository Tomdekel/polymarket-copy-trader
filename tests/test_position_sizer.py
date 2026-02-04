"""Tests for position sizer module."""
import pytest

from position_sizer import PositionSizer, SizedPosition


class TestPositionSizer:
    """Test PositionSizer class."""

    def test_calculate_proportional_positions(self, sample_config):
        """Test proportional position sizing."""
        sizer = PositionSizer(10000, sample_config)

        target_positions = [
            {"market": "0xabc123", "size": 100, "value": 500},
            {"market": "0xdef456", "size": 50, "value": 250},
        ]
        target_portfolio_value = 1000  # Target has $1000 portfolio

        result = sizer.calculate_positions(
            target_portfolio_value,
            target_positions,
            our_current_positions=[],
        )

        assert len(result) == 2
        assert all(isinstance(p, SizedPosition) for p in result)

        # First position is 50% of target portfolio, scaled to our $10k budget
        pos1 = next(p for p in result if p.market == "0xabc123")
        assert pos1.action == "BUY"
        assert pos1.target_percentage == 0.5
        # Our size should be 50% of $10k = $5000, but capped at 15% = $1500
        assert pos1.our_size == 1500  # Capped by max_position_pct

    def test_position_below_minimum_skipped(self, sample_config):
        """Test that positions below minimum percentage are skipped."""
        sizer = PositionSizer(10000, sample_config)

        target_positions = [
            {"market": "0xabc123", "size": 1, "value": 5},  # 0.5% of target
        ]
        target_portfolio_value = 1000

        result = sizer.calculate_positions(
            target_portfolio_value,
            target_positions,
            our_current_positions=[],
        )

        # 0.5% is below 1% minimum, so size should be 0
        assert result[0].our_size == 0
        assert result[0].action == "HOLD"

    def test_sell_when_target_exits(self, sample_config):
        """Test SELL action when target no longer holds position."""
        sizer = PositionSizer(10000, sample_config)

        # Target has no positions now
        target_positions = []
        target_portfolio_value = 1000

        # But we have an existing position
        our_positions = [
            {"market": "0xabc123", "size": 500, "value": 500},
        ]

        result = sizer.calculate_positions(
            target_portfolio_value,
            target_positions,
            our_positions,
        )

        # No actions since target has no positions to mirror
        assert len(result) == 0

    def test_hold_when_within_threshold(self, sample_config):
        """Test HOLD action when position is close to target."""
        sizer = PositionSizer(10000, sample_config)

        target_positions = [
            {"market": "0xabc123", "size": 100, "value": 1000},  # 10% of portfolio
        ]
        target_portfolio_value = 10000

        # We already have roughly 10% position
        our_positions = [
            {"market": "0xabc123", "size": 1000, "value": 1000},
        ]

        result = sizer.calculate_positions(
            target_portfolio_value,
            target_positions,
            our_positions,
        )

        pos = next(p for p in result if p.market == "0xabc123")
        assert pos.action == "HOLD"

    def test_rebalance_when_significantly_different(self, sample_config):
        """Test rebalancing when position differs significantly from target."""
        sizer = PositionSizer(10000, sample_config)

        target_positions = [
            {"market": "0xabc123", "size": 100, "value": 1500},  # 15% of portfolio
        ]
        target_portfolio_value = 10000

        # We have only 5% position (significantly different)
        our_positions = [
            {"market": "0xabc123", "size": 500, "value": 500},
        ]

        result = sizer.calculate_positions(
            target_portfolio_value,
            target_positions,
            our_positions,
        )

        pos = next(p for p in result if p.market == "0xabc123")
        assert pos.action == "BUY"  # Need to increase position


class TestPositionSizerEdgeCases:
    """Test edge cases in position sizing."""

    def test_zero_target_portfolio_value(self, sample_config):
        """Test handling of zero target portfolio value."""
        sizer = PositionSizer(10000, sample_config)

        target_positions = [
            {"market": "0xabc123", "size": 100, "value": 500},
        ]

        result = sizer.calculate_positions(
            0,  # Zero portfolio value
            target_positions,
            our_current_positions=[],
        )

        # Should handle gracefully
        assert len(result) == 1
        assert result[0].target_percentage == 0

    def test_zero_budget(self, sample_config):
        """Test handling of zero budget."""
        sizer = PositionSizer(0, sample_config)

        target_positions = [
            {"market": "0xabc123", "size": 100, "value": 500},
        ]

        result = sizer.calculate_positions(
            1000,
            target_positions,
            our_current_positions=[],
        )

        # Should not crash, positions should be zero
        assert all(p.our_size == 0 for p in result)

    def test_empty_target_positions(self, sample_config):
        """Test with empty target positions list."""
        sizer = PositionSizer(10000, sample_config)

        result = sizer.calculate_positions(
            1000,
            [],
            our_current_positions=[],
        )

        assert result == []
