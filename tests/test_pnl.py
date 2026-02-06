"""Tests for authoritative P&L module."""
import pytest
from pnl import (
    compute_cost_basis,
    compute_current_value,
    compute_shares,
    compute_unrealized_pnl,
    compute_unrealized_pnl_from_size,
    compute_realized_pnl,
    compute_realized_pnl_from_size,
    compute_proceeds,
    compute_proceeds_from_size,
    compute_position_value,
    compute_position_value_from_size,
    compute_pnl_percentage,
    reconcile_portfolio,
    reconcile_trade_ledger,
    check_trade_integrity,
    validate_price,
)


class TestComputeShares:
    """Test share computation from USD investment."""

    def test_normal_price(self):
        """$100 at $0.50 = 200 shares."""
        assert compute_shares(100, 0.50) == 200

    def test_low_price_market(self):
        """Low price markets like $0.02 should work correctly.
        $100 at $0.02 = 5000 shares.
        """
        assert compute_shares(100, 0.02) == 5000

    def test_very_low_price_market(self):
        """Very low price like $0.01.
        $100 at $0.01 = 10000 shares.
        """
        assert compute_shares(100, 0.01) == 10000

    def test_high_price_market(self):
        """High price market like $0.95.
        $100 at $0.95 = ~105.26 shares.
        """
        assert compute_shares(100, 0.95) == pytest.approx(105.26, rel=0.01)

    def test_zero_price(self):
        """Zero price should return 0 shares (avoid division by zero)."""
        assert compute_shares(100, 0) == 0

    def test_negative_price(self):
        """Negative price should return 0 shares."""
        assert compute_shares(100, -0.5) == 0

    def test_zero_size(self):
        """Zero investment returns 0 shares."""
        assert compute_shares(0, 0.50) == 0

    def test_small_investment(self):
        """Small investment like $1."""
        assert compute_shares(1, 0.50) == 2


class TestUnrealizedPnL:
    """Test unrealized P&L calculations."""

    def test_profit_direct(self):
        """Direct shares calculation: 200 shares, entry $0.50, current $0.60."""
        pnl = compute_unrealized_pnl(200, 0.50, 0.60)
        assert pnl == pytest.approx(20, rel=0.01)

    def test_loss_direct(self):
        """Direct shares calculation: 200 shares, entry $0.50, current $0.40."""
        pnl = compute_unrealized_pnl(200, 0.50, 0.40)
        assert pnl == pytest.approx(-20, rel=0.01)

    def test_profit_from_size(self):
        """$100 at $0.50 = 200 shares, price goes to $0.60.
        200 * ($0.60 - $0.50) = $20 profit.
        """
        pnl = compute_unrealized_pnl_from_size(100, 0.50, 0.60)
        assert pnl == pytest.approx(20, rel=0.01)

    def test_loss_from_size(self):
        """$100 at $0.50 = 200 shares, price goes to $0.40.
        200 * ($0.40 - $0.50) = -$20 loss.
        """
        pnl = compute_unrealized_pnl_from_size(100, 0.50, 0.40)
        assert pnl == pytest.approx(-20, rel=0.01)

    def test_low_price_profit(self):
        """$100 at $0.02 = 5000 shares, price goes to $0.04.
        5000 * ($0.04 - $0.02) = $100 profit (100% gain).
        """
        pnl = compute_unrealized_pnl_from_size(100, 0.02, 0.04)
        assert pnl == pytest.approx(100, rel=0.01)

    def test_low_price_loss(self):
        """$100 at $0.02 = 5000 shares, price goes to $0.01.
        5000 * ($0.01 - $0.02) = -$50 loss (50% loss).
        """
        pnl = compute_unrealized_pnl_from_size(100, 0.02, 0.01)
        assert pnl == pytest.approx(-50, rel=0.01)

    def test_breakeven(self):
        """No change in price = no P&L."""
        pnl = compute_unrealized_pnl_from_size(100, 0.50, 0.50)
        assert pnl == pytest.approx(0, abs=0.001)

    def test_zero_entry_price(self):
        """Zero entry price should return 0 (avoid division by zero)."""
        pnl = compute_unrealized_pnl_from_size(100, 0, 0.50)
        assert pnl == 0


class TestRealizedPnL:
    """Test realized P&L calculations."""

    def test_profit(self):
        """200 shares, bought at $0.50, sold at $0.75.
        200 * ($0.75 - $0.50) = $50 profit.
        """
        shares = compute_shares(100, 0.50)  # 200 shares
        pnl = compute_realized_pnl(shares, 0.50, 0.75)
        assert pnl == pytest.approx(50, rel=0.01)

    def test_loss(self):
        """200 shares, bought at $0.50, sold at $0.25.
        200 * ($0.25 - $0.50) = -$50 loss.
        """
        shares = compute_shares(100, 0.50)  # 200 shares
        pnl = compute_realized_pnl(shares, 0.50, 0.25)
        assert pnl == pytest.approx(-50, rel=0.01)

    def test_profit_from_size(self):
        """$100 at $0.50, exit at $0.75 = $50 profit."""
        pnl = compute_realized_pnl_from_size(100, 0.50, 0.75)
        assert pnl == pytest.approx(50, rel=0.01)

    def test_loss_from_size(self):
        """$100 at $0.50, exit at $0.25 = -$50 loss."""
        pnl = compute_realized_pnl_from_size(100, 0.50, 0.25)
        assert pnl == pytest.approx(-50, rel=0.01)

    def test_low_price_profit(self):
        """$100 at $0.02 = 5000 shares, exit at $0.06.
        5000 * ($0.06 - $0.02) = $200 profit.
        """
        pnl = compute_realized_pnl_from_size(100, 0.02, 0.06)
        assert pnl == pytest.approx(200, rel=0.01)

    def test_market_resolution_win(self):
        """Market resolves YES at price $1.00.
        $100 at $0.50 = 200 shares, exit at $1.00.
        200 * ($1.00 - $0.50) = $100 profit.
        """
        pnl = compute_realized_pnl_from_size(100, 0.50, 1.00)
        assert pnl == pytest.approx(100, rel=0.01)

    def test_market_resolution_loss(self):
        """Market resolves NO (our YES position becomes worthless).
        $100 at $0.50 = 200 shares, exit at $0.00.
        200 * ($0.00 - $0.50) = -$100 loss (total loss).
        """
        pnl = compute_realized_pnl_from_size(100, 0.50, 0.00)
        assert pnl == pytest.approx(-100, rel=0.01)


class TestProceeds:
    """Test proceeds calculations."""

    def test_proceeds(self):
        """200 shares at exit price $0.75 = $150 proceeds."""
        shares = compute_shares(100, 0.50)  # 200 shares
        proceeds = compute_proceeds(shares, 0.75)
        assert proceeds == pytest.approx(150, rel=0.01)

    def test_proceeds_from_size(self):
        """$100 at $0.50, exit at $0.75 = $150 proceeds."""
        proceeds = compute_proceeds_from_size(100, 0.50, 0.75)
        assert proceeds == pytest.approx(150, rel=0.01)

    def test_proceeds_at_loss(self):
        """$100 at $0.50, exit at $0.25 = $50 proceeds (lost $50)."""
        proceeds = compute_proceeds_from_size(100, 0.50, 0.25)
        assert proceeds == pytest.approx(50, rel=0.01)

    def test_proceeds_low_price_market(self):
        """$100 at $0.02, exit at $0.04 = $200 proceeds."""
        proceeds = compute_proceeds_from_size(100, 0.02, 0.04)
        assert proceeds == pytest.approx(200, rel=0.01)


class TestPositionValue:
    """Test position value calculations."""

    def test_position_value(self):
        """200 shares at $0.60 = $120 value."""
        value = compute_position_value(200, 0.60)
        assert value == pytest.approx(120, rel=0.01)

    def test_position_value_from_size(self):
        """$100 at $0.50 = 200 shares, current price $0.60 = $120 value."""
        value = compute_position_value_from_size(100, 0.50, 0.60)
        assert value == pytest.approx(120, rel=0.01)

    def test_position_value_low_price(self):
        """$100 at $0.02 = 5000 shares, current price $0.03 = $150 value."""
        value = compute_position_value_from_size(100, 0.02, 0.03)
        assert value == pytest.approx(150, rel=0.01)


class TestPnLPercentage:
    """Test P&L percentage calculations."""

    def test_positive_percentage(self):
        """$20 profit on $100 investment = 20%."""
        pct = compute_pnl_percentage(20, 100)
        assert pct == pytest.approx(20, rel=0.01)

    def test_negative_percentage(self):
        """-$50 loss on $100 investment = -50%."""
        pct = compute_pnl_percentage(-50, 100)
        assert pct == pytest.approx(-50, rel=0.01)

    def test_zero_cost_basis(self):
        """Zero cost basis should return 0 (avoid division by zero)."""
        pct = compute_pnl_percentage(20, 0)
        assert pct == 0

    def test_negative_cost_basis(self):
        """Negative cost basis should return 0."""
        pct = compute_pnl_percentage(20, -100)
        assert pct == 0

    def test_100_percent_gain(self):
        """$100 profit on $100 investment = 100%."""
        pct = compute_pnl_percentage(100, 100)
        assert pct == pytest.approx(100, rel=0.01)


class TestReconciliation:
    """Test portfolio reconciliation."""

    def test_reconcile_portfolio_with_shares(self):
        """Reconcile with pre-computed shares."""
        positions = [
            {'shares': 100, 'current_price': 0.50},
            {'shares': 200, 'current_price': 0.25},
        ]
        result = reconcile_portfolio(cash=500, positions=positions)

        assert result['cash'] == 500
        # 100 * 0.50 + 200 * 0.25 = 50 + 50 = 100
        assert result['position_value'] == pytest.approx(100, rel=0.01)
        assert result['total_value'] == pytest.approx(600, rel=0.01)
        assert result['position_count'] == 2


class TestLedgerReconciliation:
    def test_reconcile_trade_ledger_equity_identity(self):
        trades = [
            {"id": 1, "status": "open", "size": 100, "price": 0.50, "current_price": 0.60},
            {"id": 2, "status": "closed", "size": 50, "price": 0.25, "sell_price": 0.20},
        ]
        result = reconcile_trade_ledger(cash=890, trades=trades, starting_equity=1000)
        assert result["total_value"] == pytest.approx(result["cash"] + result["total_open_value"], rel=1e-9)
        assert result["equity_pnl"] == pytest.approx(
            result["total_realized"] + result["total_unrealized"],
            rel=1e-9,
        )


class TestAccountingPrimitives:
    def test_cost_basis_and_current_value(self):
        shares = compute_shares(100, 0.03)
        assert compute_cost_basis(shares, 0.03) == pytest.approx(100, rel=1e-9)
        assert compute_current_value(shares, 0.05) == pytest.approx(166.6666667, rel=1e-6)

    def test_reconcile_portfolio_with_trade_data(self):
        """Reconcile with raw trade data (size and price)."""
        positions = [
            {'size': 100, 'price': 0.50, 'current_price': 0.60},  # 200 shares at 0.60 = 120
            {'size': 50, 'price': 0.25, 'current_price': 0.30},   # 200 shares at 0.30 = 60
        ]
        result = reconcile_portfolio(cash=300, positions=positions)

        assert result['cash'] == 300
        assert result['position_value'] == pytest.approx(180, rel=0.01)
        assert result['total_value'] == pytest.approx(480, rel=0.01)
        assert result['position_count'] == 2

    def test_reconcile_empty_portfolio(self):
        """Reconcile with no positions."""
        result = reconcile_portfolio(cash=1000, positions=[])

        assert result['cash'] == 1000
        assert result['position_value'] == 0
        assert result['total_value'] == 1000
        assert result['position_count'] == 0

    def test_reconcile_fallback_to_cost_basis(self):
        """Fallback to cost basis when entry price is zero."""
        positions = [
            {'size': 100, 'price': 0, 'current_price': 0.50},  # Invalid entry, use size
        ]
        result = reconcile_portfolio(cash=500, positions=positions)

        assert result['cash'] == 500
        assert result['position_value'] == pytest.approx(100, rel=0.01)
        assert result['total_value'] == pytest.approx(600, rel=0.01)


class TestIntegrityChecks:
    """Test trade data integrity checks."""

    def test_valid_trade(self):
        """Valid trade with matching shares."""
        trade = {'size': 100, 'entry_price': 0.50, 'shares': 200}
        errors = check_trade_integrity(trade)
        assert errors == []

    def test_valid_trade_price_key(self):
        """Valid trade using 'price' key instead of 'entry_price'."""
        trade = {'size': 100, 'price': 0.50, 'shares': 200}
        errors = check_trade_integrity(trade)
        assert errors == []

    def test_invalid_size_zero(self):
        """Zero size should be flagged."""
        trade = {'size': 0, 'entry_price': 0.50}
        errors = check_trade_integrity(trade)
        assert any('size' in e.lower() for e in errors)

    def test_invalid_size_negative(self):
        """Negative size should be flagged."""
        trade = {'size': -100, 'entry_price': 0.50}
        errors = check_trade_integrity(trade)
        assert any('size' in e.lower() for e in errors)

    def test_invalid_entry_price_zero(self):
        """Zero entry price should be flagged."""
        trade = {'size': 100, 'entry_price': 0}
        errors = check_trade_integrity(trade)
        assert any('entry_price' in e.lower() for e in errors)

    def test_invalid_entry_price_negative(self):
        """Negative entry price should be flagged."""
        trade = {'size': 100, 'entry_price': -0.50}
        errors = check_trade_integrity(trade)
        assert any('entry_price' in e.lower() for e in errors)

    def test_shares_mismatch(self):
        """Mismatched shares should be flagged."""
        trade = {'size': 100, 'entry_price': 0.50, 'shares': 999}  # Should be 200
        errors = check_trade_integrity(trade)
        assert any('mismatch' in e.lower() for e in errors)

    def test_shares_close_enough(self):
        """Shares within tolerance should pass."""
        trade = {'size': 100, 'entry_price': 0.50, 'shares': 200.00001}  # Very close to 200
        errors = check_trade_integrity(trade)
        assert errors == []

    def test_no_shares_field(self):
        """Trade without shares field should pass if other fields valid."""
        trade = {'size': 100, 'entry_price': 0.50}  # No shares field
        errors = check_trade_integrity(trade)
        assert errors == []


class TestValidatePrice:
    """Test price validation."""

    def test_valid_price(self):
        """Valid probability price between 0 and 1."""
        errors = validate_price(0.50)
        assert errors == []

    def test_zero_price(self):
        """Zero is valid (market resolved NO)."""
        errors = validate_price(0.0)
        assert errors == []

    def test_one_price(self):
        """1.0 is valid (market resolved YES)."""
        errors = validate_price(1.0)
        assert errors == []

    def test_negative_price(self):
        """Negative price is invalid."""
        errors = validate_price(-0.10)
        assert any('negative' in e.lower() for e in errors)

    def test_price_exceeds_one(self):
        """Price > 1.0 is invalid for probability markets."""
        errors = validate_price(1.50)
        assert any('exceeds' in e.lower() for e in errors)


class TestLowPriceMarketScenarios:
    """Comprehensive tests for low-price market edge cases."""

    def test_penny_market_full_cycle(self):
        """Test a complete trade cycle in a $0.01 market.

        Scenario: Buy at $0.01, price goes to $0.05, then exit.
        - Investment: $100 at $0.01 = 10000 shares
        - Unrealized at $0.05: 10000 * ($0.05 - $0.01) = $400 profit
        - Realized at $0.05: same $400 profit
        - Proceeds: 10000 * $0.05 = $500
        """
        size = 100
        entry = 0.01
        current = 0.05

        shares = compute_shares(size, entry)
        assert shares == 10000

        unrealized = compute_unrealized_pnl_from_size(size, entry, current)
        assert unrealized == pytest.approx(400, rel=0.01)

        realized = compute_realized_pnl_from_size(size, entry, current)
        assert realized == pytest.approx(400, rel=0.01)

        proceeds = compute_proceeds_from_size(size, entry, current)
        assert proceeds == pytest.approx(500, rel=0.01)

        pnl_pct = compute_pnl_percentage(realized, size)
        assert pnl_pct == pytest.approx(400, rel=0.01)  # 400% gain

    def test_two_cent_market_losing_trade(self):
        """Test a losing trade in a $0.02 market.

        Scenario: Buy at $0.02, price drops to $0.005.
        - Investment: $100 at $0.02 = 5000 shares
        - Unrealized at $0.005: 5000 * ($0.005 - $0.02) = -$75 loss
        - This is a 75% loss
        """
        size = 100
        entry = 0.02
        current = 0.005

        shares = compute_shares(size, entry)
        assert shares == 5000

        unrealized = compute_unrealized_pnl_from_size(size, entry, current)
        assert unrealized == pytest.approx(-75, rel=0.01)

        pnl_pct = compute_pnl_percentage(unrealized, size)
        assert pnl_pct == pytest.approx(-75, rel=0.01)  # 75% loss

    def test_market_resolution_at_extremes(self):
        """Test market resolution (price goes to 0 or 1).

        Scenario A: Bet YES at $0.02, market resolves YES (price = $1.00)
        - Investment: $100 at $0.02 = 5000 shares
        - Realized: 5000 * ($1.00 - $0.02) = $4900 profit (4900% gain!)

        Scenario B: Bet YES at $0.98, market resolves NO (price = $0.00)
        - Investment: $100 at $0.98 = ~102 shares
        - Realized: 102 * ($0.00 - $0.98) = -$100 loss (total loss)
        """
        # Scenario A: Long shot wins
        realized_a = compute_realized_pnl_from_size(100, 0.02, 1.00)
        assert realized_a == pytest.approx(4900, rel=0.01)

        # Scenario B: Favorite loses
        realized_b = compute_realized_pnl_from_size(100, 0.98, 0.00)
        assert realized_b == pytest.approx(-100, rel=0.01)
