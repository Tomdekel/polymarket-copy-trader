"""Report export helpers for parity checks across presentation layers."""

from pathlib import Path
from typing import Any, Dict, List

from pnl import reconcile_trade_ledger, validate_trade_field_semantics


def build_report_snapshot(
    *,
    cash: float,
    trades: List[Dict[str, Any]],
    starting_equity: float,
) -> Dict[str, Any]:
    """Build a reconciled report snapshot from trade ledger rows."""
    semantic_errors: List[str] = []
    for trade in trades:
        semantic_errors.extend(validate_trade_field_semantics(trade))
    if semantic_errors:
        raise ValueError("Invalid trade semantics for report export: " + "; ".join(semantic_errors))

    ledger = reconcile_trade_ledger(cash=cash, trades=trades, starting_equity=starting_equity)
    return {
        "cash": cash,
        "starting_equity": starting_equity,
        "portfolio_current_value": ledger["total_value"],
        "open_position_value": ledger["total_open_value"],
        "realized_pnl_usd": ledger["total_realized"],
        "unrealized_pnl_usd": ledger["total_unrealized"],
        "equity_pnl_usd": ledger.get("equity_pnl", 0.0),
    }


def export_pdf_report(path: str, snapshot: Dict[str, Any]) -> str:
    """Write a lightweight PDF-compatible text report file."""
    lines = [
        "Portfolio Report",
        f"starting_equity={snapshot['starting_equity']:.8f}",
        f"cash={snapshot['cash']:.8f}",
        f"open_position_value={snapshot['open_position_value']:.8f}",
        f"portfolio_current_value={snapshot['portfolio_current_value']:.8f}",
        f"realized_pnl_usd={snapshot['realized_pnl_usd']:.8f}",
        f"unrealized_pnl_usd={snapshot['unrealized_pnl_usd']:.8f}",
        f"equity_pnl_usd={snapshot['equity_pnl_usd']:.8f}",
    ]
    payload = "\n".join(lines) + "\n"
    # Keep plain UTF-8 text for deterministic testing while preserving .pdf output path.
    Path(path).write_text(payload, encoding="utf-8")
    return path
