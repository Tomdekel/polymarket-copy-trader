"""
Authoritative P&L calculation module.
Single source of truth for all P&L math in the copy trader.

This module provides consistent, tested functions for:
- Share calculations from USD investment
- Unrealized P&L for open positions
- Realized P&L for closed positions
- Portfolio reconciliation
- Trade data integrity checks
"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


def compute_shares(size: float, entry_price: float) -> float:
    """
    Compute number of shares from USD size and entry price.

    Args:
        size: USD invested (cost basis)
        entry_price: Price per share when bought

    Returns:
        Number of shares
    """
    if entry_price <= 0:
        return 0.0
    return size / entry_price


def assert_price_probability(price: Optional[float], field_name: str) -> None:
    """Validate that market prices stay in [0, 1] when provided."""
    if price is None:
        return
    if price < 0 or price > 1:
        raise ValueError(f"{field_name} must be in [0, 1], got {price}")


def assert_shares_consistent(
    *,
    shares: float,
    entry_price: float,
    cost_basis_usd: float,
    eps: float = 1e-6,
) -> None:
    """Guard against mixing up shares with USD size."""
    if shares <= 0:
        raise ValueError(f"shares must be positive, got {shares}")
    expected_cost = shares * entry_price
    if abs(expected_cost - cost_basis_usd) > eps:
        raise ValueError(
            f"shares/cost_basis mismatch: shares*entry_price={expected_cost}, cost_basis_usd={cost_basis_usd}. "
            "Do not use size_usd where shares are required."
        )


def compute_cost_basis(shares: float, entry_price: float) -> float:
    """Compute USD cost basis from share quantity and entry price."""
    return shares * entry_price


def compute_current_value(shares: float, current_price: float) -> float:
    """Compute current USD value from share quantity and current price."""
    return shares * current_price


def compute_unrealized_pnl(
    shares: float,
    entry_price: float,
    current_price: Optional[float],
    context: str = "",
) -> float:
    """
    Compute unrealized P&L for an open position.

    Args:
        shares: Number of shares held
        entry_price: Price per share when bought
        current_price: Current market price

    Returns:
        Unrealized P&L in USD
    """
    if current_price is None:
        if context:
            logger.warning("Missing current_price for %s; unrealized P&L set to 0", context)
        else:
            logger.warning("Missing current_price; unrealized P&L set to 0")
        return 0.0
    return shares * (current_price - entry_price)


def compute_unrealized_pnl_from_size(size: float, entry_price: float, current_price: float) -> float:
    """
    Compute unrealized P&L directly from size (convenience function).

    This is the most commonly used function for calculating P&L from
    trade records where we store USD invested rather than shares.

    Args:
        size: USD invested (cost basis)
        entry_price: Price per share when bought
        current_price: Current market price

    Returns:
        Unrealized P&L in USD
    """
    shares = compute_shares(size, entry_price)
    return compute_unrealized_pnl(shares, entry_price, current_price)


def compute_realized_pnl(
    shares: float,
    entry_price: float,
    exit_price: Optional[float],
    context: str = "",
) -> float:
    """
    Compute realized P&L for a closed position.

    Args:
        shares: Number of shares sold
        entry_price: Price per share when bought
        exit_price: Price per share when sold

    Returns:
        Realized P&L in USD
    """
    if exit_price is None:
        if context:
            logger.warning("Missing exit_price for %s; realized P&L set to 0", context)
        else:
            logger.warning("Missing exit_price; realized P&L set to 0")
        return 0.0
    return shares * (exit_price - entry_price)


def compute_realized_pnl_from_size(size: float, entry_price: float, exit_price: float) -> float:
    """
    Compute realized P&L directly from size (convenience function).

    Args:
        size: USD invested (cost basis)
        entry_price: Price per share when bought
        exit_price: Price per share when sold

    Returns:
        Realized P&L in USD
    """
    shares = compute_shares(size, entry_price)
    return compute_realized_pnl(shares, entry_price, exit_price)


def compute_proceeds(shares: float, exit_price: Optional[float], context: str = "") -> float:
    """
    Compute proceeds from selling shares.

    Args:
        shares: Number of shares sold
        exit_price: Price per share when sold

    Returns:
        Proceeds in USD
    """
    if exit_price is None:
        if context:
            logger.warning("Missing exit_price for %s; proceeds set to 0", context)
        else:
            logger.warning("Missing exit_price; proceeds set to 0")
        return 0.0
    return shares * exit_price


def compute_proceeds_from_size(size: float, entry_price: float, exit_price: float) -> float:
    """
    Compute proceeds directly from size (convenience function).

    Args:
        size: USD invested (cost basis)
        entry_price: Price per share when bought
        exit_price: Price per share when sold

    Returns:
        Proceeds in USD
    """
    shares = compute_shares(size, entry_price)
    return compute_proceeds(shares, exit_price)


def compute_position_value(shares: float, current_price: float) -> float:
    """
    Compute current value of a position.

    Args:
        shares: Number of shares held
        current_price: Current market price

    Returns:
        Position value in USD
    """
    return compute_current_value(shares, current_price)


def compute_position_value_from_size(size: float, entry_price: float, current_price: float) -> float:
    """
    Compute current position value directly from size (convenience function).

    Args:
        size: USD invested (cost basis)
        entry_price: Price per share when bought
        current_price: Current market price

    Returns:
        Position value in USD
    """
    shares = compute_shares(size, entry_price)
    return compute_position_value(shares, current_price)


def compute_pnl_percentage(pnl: float, cost_basis: float) -> float:
    """
    Compute P&L as a percentage of cost basis.

    Args:
        pnl: P&L in USD
        cost_basis: Original investment (size)

    Returns:
        P&L percentage (e.g., 10.0 for 10%)
    """
    if cost_basis <= 0:
        return 0.0
    return (pnl / cost_basis) * 100


def reconcile_portfolio(cash: float, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Reconcile portfolio totals.

    This function validates that portfolio values are internally consistent
    and provides a summary of portfolio state.

    Args:
        cash: Available cash
        positions: List of dicts with 'shares' and 'current_price'
                  (or 'size', 'entry_price', 'current_price' for raw trade data)

    Returns:
        Dict with reconciliation results:
        - cash: Available cash
        - position_value: Total value of all positions
        - total_value: cash + position_value
        - position_count: Number of positions
    """
    position_value = 0.0

    for p in positions:
        if 'shares' in p and 'current_price' in p:
            # Direct shares provided
            position_value += compute_position_value(p['shares'], p['current_price'])
        elif 'size' in p and 'price' in p:
            # Raw trade data - compute shares from size and entry price
            entry_price = p.get('price', 0)
            current_price = p.get('current_price', entry_price)
            if entry_price > 0:
                shares = compute_shares(p['size'], entry_price)
                position_value += compute_position_value(shares, current_price)
            else:
                # Fallback to cost basis if no valid price
                position_value += p.get('size', 0)

    total_value = cash + position_value

    return {
        'cash': cash,
        'position_value': position_value,
        'total_value': total_value,
        'position_count': len(positions)
    }


def reconcile_trade_ledger(
    cash: float,
    trades: List[Dict[str, Any]],
    starting_equity: Optional[float] = None,
) -> Dict[str, Any]:
    """Reconcile portfolio totals from raw trade ledger rows."""
    total_open_value = 0.0
    total_unrealized = 0.0
    total_realized = 0.0
    open_positions = 0
    closed_positions = 0

    for trade in trades:
        status = (trade.get("status") or "open").lower()
        trade_id = trade.get("id", "unknown")
        size_usd = float(trade.get("size") or 0.0)
        entry_price = float(trade.get("price") or 0.0)
        shares = float(trade.get("shares") or 0.0)
        if shares <= 0 and entry_price > 0:
            shares = compute_shares(size_usd, entry_price)

        if status == "open":
            open_positions += 1
            current_price = trade.get("current_price")
            if current_price is None:
                logger.warning(
                    "Trade %s missing current_price; using entry_price for current value and unrealized=0",
                    trade_id,
                )
                current_price = entry_price
            current_price = float(current_price)
            current_value = compute_current_value(shares, current_price)
            unrealized = compute_unrealized_pnl(
                shares=shares,
                entry_price=entry_price,
                current_price=current_price,
                context=f"trade_id={trade_id}",
            )
            total_open_value += current_value
            total_unrealized += unrealized
            continue

        closed_positions += 1
        exit_price = trade.get("sell_price")
        proceeds = compute_proceeds(shares, exit_price, context=f"trade_id={trade_id}")
        realized = compute_realized_pnl(
            shares=shares,
            entry_price=entry_price,
            exit_price=exit_price,
            context=f"trade_id={trade_id}",
        )
        total_realized += realized

        if proceeds <= 0 and exit_price is not None:
            # Keep variable to make debugging easier when reconciliation drifts.
            logger.debug(
                "Closed trade %s has non-positive proceeds (shares=%s, exit=%s)",
                trade_id,
                shares,
                exit_price,
            )

    total_value = cash + total_open_value
    result = {
        "cash": cash,
        "total_open_value": total_open_value,
        "total_unrealized": total_unrealized,
        "total_realized": total_realized,
        "total_value": total_value,
        "open_positions": open_positions,
        "closed_positions": closed_positions,
    }
    if starting_equity is not None:
        result["equity_pnl"] = total_value - starting_equity
    return result


def validate_trade_field_semantics(trade: Dict[str, Any], eps: float = 1e-6) -> List[str]:
    """Validate strict accounting field semantics on a trade row."""
    errors: List[str] = []
    trade_id = trade.get("id", "unknown")
    status = (trade.get("status") or "open").lower()

    shares = float(trade.get("shares") or 0.0)
    entry_price = trade.get("price")
    current_price = trade.get("current_price")
    exit_price = trade.get("sell_price")
    size_usd = float(trade.get("size") or 0.0)

    if entry_price is None:
        errors.append(f"trade_id={trade_id} missing entry_price")
    else:
        try:
            assert_price_probability(float(entry_price), "entry_price")
        except ValueError as e:
            errors.append(f"trade_id={trade_id} {e}")

    if status == "open":
        if current_price is None:
            errors.append(f"trade_id={trade_id} open trade missing current_price")
        if exit_price is not None:
            errors.append(f"trade_id={trade_id} open trade must not have exit_price")
    else:
        if exit_price is None:
            errors.append(f"trade_id={trade_id} closed trade missing exit_price")
        if current_price is not None:
            errors.append(f"trade_id={trade_id} closed trade must not have current_price")

    if size_usd <= 0:
        errors.append(f"trade_id={trade_id} invalid cost_basis_usd={size_usd}")

    derived_shares = shares
    if derived_shares <= 0 and entry_price is not None:
        try:
            ep = float(entry_price)
            if ep > 0 and size_usd > 0:
                derived_shares = compute_shares(size_usd, ep)
        except (TypeError, ValueError):
            pass
    if derived_shares <= 0:
        errors.append(f"trade_id={trade_id} invalid shares={shares}")

    if not errors and entry_price is not None:
        try:
            assert_shares_consistent(
                shares=derived_shares,
                entry_price=float(entry_price),
                cost_basis_usd=size_usd,
                eps=eps,
            )
        except ValueError as e:
            errors.append(f"trade_id={trade_id} {e}")
    return errors


def check_trade_integrity(trade: Dict[str, Any]) -> List[str]:
    """
    Check a trade record for data integrity issues.

    This function validates that trade data is consistent and can be
    used for P&L calculations.

    Args:
        trade: Trade record dict with expected keys:
              - size: USD invested (required)
              - entry_price or price: Entry price (required)
              - shares: Number of shares (optional, validated if present)

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    size = trade.get('size', 0)
    # Support both 'entry_price' and 'price' keys
    entry_price = trade.get('entry_price') or trade.get('price', 0)
    shares = trade.get('shares')

    if size <= 0:
        errors.append(f"Invalid size: {size}")

    if entry_price <= 0:
        errors.append(f"Invalid entry_price: {entry_price}")

    if shares is not None and entry_price > 0:
        expected_shares = compute_shares(size, entry_price)
        # Use relative tolerance for floating point comparison
        if abs(shares - expected_shares) > max(0.0001, abs(expected_shares) * 0.0001):
            errors.append(f"Shares mismatch: stored={shares}, computed={expected_shares}")

    return errors


def validate_price(price: float, context: str = "price") -> List[str]:
    """
    Validate a price value.

    Args:
        price: The price to validate
        context: Description of what price this is (for error messages)

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    if price < 0:
        errors.append(f"Negative {context}: {price}")
    elif price > 1.0:
        # Polymarket prices are between 0 and 1 (probability)
        errors.append(f"{context} exceeds 1.0 (max for probability market): {price}")

    return errors
