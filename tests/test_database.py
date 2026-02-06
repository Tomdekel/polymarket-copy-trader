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
        """Test that BUY trades reduce cash balance.

        Note: 'size' is USD invested, so cash is reduced by 'size' directly.
        """
        temp_db.initialize_portfolio(10000)

        trade_id = temp_db.add_trade(
            market="0xabc123",
            side="BUY",
            size=100,  # USD invested
            price=0.5,
            target_wallet="0x123",
        )

        assert trade_id is not None
        # Cash should be reduced by size (USD invested) = 100
        assert temp_db.get_cash_balance() == 9900

    def test_add_trade_sell_increases_cash(self, temp_db):
        """Test that SELL trades increase cash balance.

        Note: 'size' is USD, so cash is increased by 'size' directly.
        """
        temp_db.initialize_portfolio(10000)

        temp_db.add_trade(
            market="0xabc123",
            side="SELL",
            size=100,  # USD
            price=0.5,
            target_wallet="0x123",
        )

        # Cash should increase by size = 100
        assert temp_db.get_cash_balance() == 10100

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
        """Test updating PnL for a trade.

        Note: 'size' is USD invested. To calculate P&L:
        - shares = size / entry_price = 100 / 0.5 = 200
        - pnl = (current_price - entry_price) * shares = (0.6 - 0.5) * 200 = 20
        """
        temp_db.initialize_portfolio(10000)

        trade_id = temp_db.add_trade(
            market="0xabc123",
            side="BUY",
            size=100,  # USD invested
            price=0.5,  # entry price
            target_wallet="0x123",
        )

        # Price went up to 0.6, should have profit
        # shares = 100 / 0.5 = 200
        # pnl = (0.6 - 0.5) * 200 = 20
        pnl = temp_db.update_trade_pnl(trade_id, 0.6)
        assert abs(pnl - 20.0) < 0.01

        # Verify PnL is stored
        positions = temp_db.get_open_positions()
        assert abs(positions[0]["pnl"] - 20.0) < 0.01

        # Verify current_price is stored
        assert positions[0]["current_price"] == 0.6

    def test_close_trade(self, temp_db):
        """Test closing a trade and realizing PnL.

        Note: 'size' is USD invested.
        - shares = size / entry_price = 100 / 0.5 = 200
        - pnl = (exit_price - entry_price) * shares = (0.6 - 0.5) * 200 = 20
        - proceeds = shares * exit_price = 200 * 0.6 = 120
        """
        temp_db.initialize_portfolio(10000)

        trade_id = temp_db.add_trade(
            market="0xabc123",
            side="BUY",
            size=100,  # USD invested
            price=0.5,  # entry price
            target_wallet="0x123",
        )

        initial_cash = temp_db.get_cash_balance()  # 9900 (10000 - 100)

        # Close at higher price
        # shares = 100 / 0.5 = 200
        # pnl = (0.6 - 0.5) * 200 = 20
        pnl = temp_db.close_trade(trade_id, 0.6)
        assert abs(pnl - 20.0) < 0.01

        # Trade should be closed
        positions = temp_db.get_open_positions()
        assert len(positions) == 0

        # Cash should increase by proceeds (200 shares * 0.6 = 120)
        assert abs(temp_db.get_cash_balance() - (initial_cash + 120)) < 0.01

        # Total PnL should be updated
        stats = temp_db.get_portfolio_stats()
        assert abs(stats["pnl_total"] - 20.0) < 0.01

    def test_close_trade_low_price_market_uses_shares_not_size(self, temp_db):
        """Low-price market must realize P&L from shares, not raw USD size."""
        temp_db.initialize_portfolio(10000)
        trade_id = temp_db.add_trade(
            market="0xlow",
            side="BUY",
            size=100,      # USD cost basis
            price=0.03,    # entry
            target_wallet="0x123",
        )

        pnl = temp_db.close_trade(trade_id, 0.05)
        # shares = 100 / 0.03 = 3333.333..., pnl = shares * 0.02 = 66.666...
        assert pnl == pytest.approx(66.666666, rel=1e-6)

    def test_close_trade_one_dollar_market(self, temp_db):
        """Near-$1 market sanity check."""
        temp_db.initialize_portfolio(10000)
        trade_id = temp_db.add_trade(
            market="0xhigh",
            side="BUY",
            size=100,
            price=0.95,
            target_wallet="0x123",
        )
        pnl = temp_db.close_trade(trade_id, 0.99)
        assert pnl == pytest.approx((100 / 0.95) * 0.04, rel=1e-6)

    def test_portfolio_reconciliation_matches_cash_plus_open_value(self, temp_db):
        """Portfolio current value must reconcile to cash + open values."""
        temp_db.initialize_portfolio(10000)
        open_trade = temp_db.add_trade(
            market="0xopen",
            side="BUY",
            size=200,
            price=0.50,
            target_wallet="0x123",
        )
        closed_trade = temp_db.add_trade(
            market="0xclosed",
            side="BUY",
            size=100,
            price=0.25,
            target_wallet="0x123",
        )
        temp_db.update_trade_pnl(open_trade, 0.70)
        temp_db.close_trade(closed_trade, 0.40)

        reconciliation = temp_db.reconcile_portfolio(starting_equity=10000)
        assert reconciliation["total_value"] == pytest.approx(
            reconciliation["cash"] + reconciliation["total_open_value"],
            rel=1e-9,
        )
        assert reconciliation["equity_pnl"] == pytest.approx(
            reconciliation["total_realized"] + reconciliation["total_unrealized"],
            rel=1e-9,
        )

    def test_partial_close_preserves_remaining_open_position(self, temp_db):
        """Partial SELL should not fully liquidate an open trade."""
        temp_db.initialize_portfolio(10000)
        trade_id = temp_db.add_trade(
            market="0xpartial",
            side="BUY",
            size=100,
            price=0.5,
            target_wallet="0x123",
        )
        pnl = temp_db.close_trade(trade_id, exit_price=0.6, close_size=40)
        assert pnl == pytest.approx((40 / 0.5) * 0.1, rel=1e-9)

        open_pos = temp_db.get_position_by_market("0xpartial")
        assert open_pos is not None
        assert open_pos["status"] == "open"
        assert open_pos["size"] == pytest.approx(60.0, rel=1e-9)

        all_trades = temp_db.get_all_trades()
        closed_trades = [t for t in all_trades if t["status"] == "closed"]
        assert len(closed_trades) == 1
        assert closed_trades[0]["size"] == pytest.approx(40.0, rel=1e-9)
        assert closed_trades[0]["current_price"] is None

    def test_reconciliation_gate_raises_on_portfolio_mismatch(self, temp_db):
        temp_db.initialize_portfolio(10000)
        trade_id = temp_db.add_trade(
            market="0xgate",
            side="BUY",
            size=100,
            price=0.50,
            target_wallet="0x123",
        )
        temp_db.update_trade_pnl(trade_id, 0.60)
        # Corrupt portfolio total_value to trigger gate failure.
        temp_db.update_portfolio(total_value=10000, cash=temp_db.get_cash_balance(), pnl_24h=0)
        with pytest.raises(AssertionError):
            temp_db.run_reconciliation_gate(mode="backtest", eps=1e-6)

    def test_reconciliation_gate_live_requires_fill_sourced_exit_prices(self, temp_db):
        temp_db.initialize_portfolio(10000)
        trade_id = temp_db.add_trade(
            market="0xlivegate",
            side="BUY",
            size=100,
            price=0.5,
            target_wallet="0x123",
        )
        temp_db.close_trade(
            trade_id,
            0.55,
            exit_price_source="quote",
            fill_price_source="quote",
        )
        with pytest.raises(RuntimeError, match="halting trading"):
            temp_db.run_reconciliation_gate(mode="live", eps=1e-6)

    def test_close_method(self, temp_db):
        """Test database close method."""
        temp_db.initialize_portfolio(1000)
        temp_db.close()

        # Check connection is closed via thread-local storage
        assert not hasattr(temp_db._local, 'conn') or temp_db._local.conn is None


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

    def test_zero_price_trade_rejected(self, temp_db):
        """Test that trades with zero price are rejected."""
        temp_db.initialize_portfolio(10000)

        with pytest.raises(ValueError, match="Trade price must be positive"):
            temp_db.add_trade(
                market="0xabc123",
                side="BUY",
                size=100,
                price=0,
                target_wallet="0x123",
            )
