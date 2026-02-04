"""Tests for database module."""
import pytest
from datetime import datetime, timedelta


class TestDatabase:
    """Test Database class functionality."""

    def test_initialize_portfolio(self, temp_db):
        """Test portfolio initialization with starting budget."""
        temp_db.initialize_portfolio(10000)

        stats = temp_db.get_portfolio_stats()
        assert stats["total_value"] == 10000
        assert stats["cash"] == 10000
        assert stats["initial_budget"] == 10000
        assert stats["pnl_total"] == 0

    def test_get_cash_balance(self, temp_db):
        """Test getting cash balance."""
        temp_db.initialize_portfolio(5000)

        balance = temp_db.get_cash_balance()
        assert balance == 5000

    def test_add_trade_buy_reduces_cash(self, temp_db):
        """Test that BUY trades reduce cash balance."""
        temp_db.initialize_portfolio(10000)

        trade_id = temp_db.add_trade(
            market="0xabc123",
            side="BUY",
            size=100,
            price=0.5,
            target_wallet="0x123",
        )

        assert trade_id is not None
        # Cash should be reduced by size * price = 100 * 0.5 = 50
        assert temp_db.get_cash_balance() == 9950

    def test_add_trade_sell_increases_cash(self, temp_db):
        """Test that SELL trades increase cash balance."""
        temp_db.initialize_portfolio(10000)

        temp_db.add_trade(
            market="0xabc123",
            side="SELL",
            size=100,
            price=0.5,
            target_wallet="0x123",
        )

        # Cash should increase by size * price = 100 * 0.5 = 50
        assert temp_db.get_cash_balance() == 10050

    def test_get_open_positions(self, temp_db):
        """Test retrieving open positions."""
        temp_db.initialize_portfolio(10000)

        temp_db.add_trade(
            market="0xabc123",
            side="BUY",
            size=100,
            price=0.5,
            target_wallet="0x123",
        )
        temp_db.add_trade(
            market="0xdef456",
            side="BUY",
            size=50,
            price=0.3,
            target_wallet="0x123",
        )

        positions = temp_db.get_open_positions()
        assert len(positions) == 2
        assert positions[0]["market"] in ["0xabc123", "0xdef456"]

    def test_get_position_by_market(self, temp_db):
        """Test getting position for specific market."""
        temp_db.initialize_portfolio(10000)

        temp_db.add_trade(
            market="0xabc123",
            side="BUY",
            size=100,
            price=0.5,
            target_wallet="0x123",
        )

        position = temp_db.get_position_by_market("0xabc123")
        assert position is not None
        assert position["market"] == "0xabc123"
        assert position["size"] == 100

        # Non-existent market should return None
        assert temp_db.get_position_by_market("0xnonexistent") is None

    def test_update_trade_pnl(self, temp_db):
        """Test updating PnL for a trade."""
        temp_db.initialize_portfolio(10000)

        trade_id = temp_db.add_trade(
            market="0xabc123",
            side="BUY",
            size=100,
            price=0.5,
            target_wallet="0x123",
        )

        # Price went up to 0.6, should have profit
        pnl = temp_db.update_trade_pnl(trade_id, 0.6)
        assert pnl == 10.0  # (0.6 - 0.5) * 100

        # Verify PnL is stored
        positions = temp_db.get_open_positions()
        assert positions[0]["pnl"] == 10.0

    def test_close_trade(self, temp_db):
        """Test closing a trade and realizing PnL."""
        temp_db.initialize_portfolio(10000)

        trade_id = temp_db.add_trade(
            market="0xabc123",
            side="BUY",
            size=100,
            price=0.5,
            target_wallet="0x123",
        )

        initial_cash = temp_db.get_cash_balance()  # 9950

        # Close at higher price
        pnl = temp_db.close_trade(trade_id, 0.6)
        assert pnl == 10.0  # (0.6 - 0.5) * 100

        # Trade should be closed
        positions = temp_db.get_open_positions()
        assert len(positions) == 0

        # Cash should increase by proceeds (100 * 0.6 = 60)
        assert temp_db.get_cash_balance() == initial_cash + 60

        # Total PnL should be updated
        stats = temp_db.get_portfolio_stats()
        assert stats["pnl_total"] == 10.0

    def test_close_method(self, temp_db):
        """Test database close method."""
        temp_db.initialize_portfolio(1000)
        temp_db.close()

        # Connection should be closed
        assert temp_db._conn is None


class TestDatabaseEdgeCases:
    """Test edge cases in database operations."""

    def test_empty_database(self, temp_db):
        """Test operations on empty database."""
        assert temp_db.get_cash_balance() == 0
        assert temp_db.get_open_positions() == []
        stats = temp_db.get_portfolio_stats()
        assert stats["total_value"] == 0

    def test_update_pnl_nonexistent_trade(self, temp_db):
        """Test updating PnL for non-existent trade."""
        temp_db.initialize_portfolio(10000)
        pnl = temp_db.update_trade_pnl(9999, 0.5)
        assert pnl == 0.0

    def test_close_nonexistent_trade(self, temp_db):
        """Test closing non-existent trade."""
        temp_db.initialize_portfolio(10000)
        pnl = temp_db.close_trade(9999, 0.5)
        assert pnl == 0.0

    def test_zero_price_trade(self, temp_db):
        """Test trade with zero price."""
        temp_db.initialize_portfolio(10000)

        trade_id = temp_db.add_trade(
            market="0xabc123",
            side="BUY",
            size=100,
            price=0,
            target_wallet="0x123",
        )

        # PnL calculation with zero price should return 0
        pnl = temp_db.update_trade_pnl(trade_id, 0.5)
        assert pnl == 0.0
