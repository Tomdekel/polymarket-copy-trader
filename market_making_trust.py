"""Trust gates and debug bundle helpers for market making experiments."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from database import Database
from execution_diagnostics import ExecutionDiagnostics
from pnl import compute_shares


def run_trust_gates(
    *,
    stage: str,
    db: Database,
    diagnostics: ExecutionDiagnostics,
    run_id: str,
    run_tag: str,
    eps: float,
    output_dir: str,
    run_meta: Dict[str, Any],
) -> None:
    issues: List[str] = []
    offending_exec: List[Dict[str, Any]] = []
    offending_trades: List[Dict[str, Any]] = []

    recon = db.reconcile_portfolio()
    cash = float(recon.get("cash") or 0.0)
    total_open = float(recon.get("total_open_value") or 0.0)
    total_value = float(recon.get("total_value") or 0.0)
    if abs(total_value - (cash + total_open)) > eps:
        issues.append(
            f"portfolio_identity_failed: total_value={total_value:.6f} cash={cash:.6f} open={total_open:.6f}"
        )

    for record in diagnostics.fetch_records(run_id=run_id, run_tag=run_tag):
        if record.get("fill_price") is None:
            continue
        order_id = record.get("order_id")
        if not order_id:
            issues.append("fill_missing_order_id")
            offending_exec.append(record)
            continue
        fill_price = record.get("fill_price")
        if fill_price is None or fill_price < 0 or fill_price > 1:
            issues.append(f"fill_price_out_of_range order_id={order_id} price={fill_price}")
            offending_exec.append(record)
        best_bid = record.get("best_bid")
        best_ask = record.get("best_ask")
        mid = record.get("mid_price")
        if best_bid is None or best_ask is None:
            issues.append(f"fill_missing_snapshot order_id={order_id}")
            offending_exec.append(record)
            continue
        if best_bid > best_ask + eps:
            issues.append(f"fill_bid_gt_ask order_id={order_id} bid={best_bid} ask={best_ask}")
            offending_exec.append(record)
        expected_mid = (best_bid + best_ask) / 2.0
        if mid is None or abs(mid - expected_mid) > eps:
            issues.append(f"fill_mid_mismatch order_id={order_id} mid={mid} expected={expected_mid}")
            offending_exec.append(record)

    for trade in db.get_all_trades():
        if (trade.get("run_id") or "") != run_id or (trade.get("run_tag") or "") != run_tag:
            continue
        status = (trade.get("status") or "open").lower()
        if status != "closed":
            continue
        entry_price = float(trade.get("price") or 0.0)
        size = float(trade.get("size") or 0.0)
        shares = float(trade.get("shares") or 0.0) or compute_shares(size, entry_price)
        exit_price = float(trade.get("sell_price") or 0.0)
        proceeds = float(trade.get("proceeds") or 0.0)
        expected_proceeds = shares * exit_price if exit_price else 0.0
        if abs(proceeds - expected_proceeds) > eps:
            issues.append(
                f"closed_cycle_proceeds_mismatch trade_id={trade.get('id')} proceeds={proceeds} expected={expected_proceeds}"
            )
            offending_trades.append(trade)

        realized = float(trade.get("realized_pnl") or 0.0)
        expected_realized = (proceeds - size) if proceeds and size else 0.0
        if abs(realized - expected_realized) > eps:
            issues.append(
                f"closed_cycle_realized_mismatch trade_id={trade.get('id')} realized={realized} expected={expected_realized}"
            )
            offending_trades.append(trade)

    if issues:
        _write_debug_bundle(
            output_dir=output_dir,
            stage=stage,
            run_id=run_id,
            run_tag=run_tag,
            run_meta=run_meta,
            issues=issues[:20],
            offending_exec=offending_exec[:20],
            offending_trades=offending_trades[:20],
        )
        raise RuntimeError(f"Trust gate failed at stage={stage}; see debug bundle in {output_dir}")


def _write_debug_bundle(
    *,
    output_dir: str,
    stage: str,
    run_id: str,
    run_tag: str,
    run_meta: Dict[str, Any],
    issues: List[str],
    offending_exec: List[Dict[str, Any]],
    offending_trades: List[Dict[str, Any]],
) -> str:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    bundle_dir = Path(output_dir) / f"debug_bundle_{run_tag}_{ts}"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "stage": stage,
        "run_id": run_id,
        "run_tag": run_tag,
        "run_meta": run_meta,
        "issues": issues,
        "offending_execution_rows": offending_exec,
        "offending_trade_rows": offending_trades,
    }
    path = bundle_dir / "bundle.json"
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return str(path)
