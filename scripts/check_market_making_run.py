#!/usr/bin/env python3
"""Acceptance checks for market making runs."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
from typing import Dict, List, Optional, Tuple


def _load_json(path: pathlib.Path) -> Dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_csv(path: pathlib.Path) -> List[Dict[str, str]]:
    import csv

    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _to_float(value: object) -> Optional[float]:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fail(reason: str) -> None:
    print(f"FAIL: {reason}")
    raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Check market making run acceptance criteria.")
    parser.add_argument("--report-json", required=True, help="Analyzer JSON output")
    parser.add_argument("--fills-csv", default=None, help="Truthful fills CSV path")
    parser.add_argument("--truthful-rate-min", type=float, default=0.99, help="Minimum truthful rate")
    parser.add_argument("--max-exposure-usd", type=float, required=True, help="Max total exposure USD")
    parser.add_argument("--max-per-market-exposure-usd", type=float, required=True, help="Max per-market exposure USD")
    parser.add_argument("--baseline", action="store_true", help="Enable baseline P&L checks")
    parser.add_argument("--baseline-fee-tolerance-usd", type=float, default=5.0, help="Tolerance for net ~= -fees")
    parser.add_argument("--warn-zero-fills-after-min", type=float, default=10.0, help="Warn if 0 fills after N minutes")
    args = parser.parse_args()

    report_path = pathlib.Path(args.report_json)
    if not report_path.exists():
        _fail(f"missing report json: {report_path}")
    report = _load_json(report_path)

    truth = report.get("truth", {})
    reconciliation_status = truth.get("reconciliation_status")
    if reconciliation_status != "pass":
        _fail(f"reconciliation_status={reconciliation_status}")

    total_rows = int(truth.get("total_rows") or 0)
    not_filled = int(truth.get("exclusions", {}).get("not_filled") or 0)
    filled_rows = total_rows - not_filled
    if filled_rows > 0:
        truthful_rate = float(truth.get("truthful_rate") or 0.0)
        if truthful_rate < args.truthful_rate_min:
            _fail(f"truthful_rate {truthful_rate:.4f} < {args.truthful_rate_min}")
    # 0 fills: nothing to verify truthfulness on — skip check.

    pnl = report.get("pnl", {})
    net_pnl = float(pnl.get("net_pnl_usd") or 0.0)
    fees = float(pnl.get("fees_usd") or 0.0)
    if args.baseline:
        diff = abs(net_pnl + fees)
        if diff > args.baseline_fee_tolerance_usd:
            _fail(f"baseline net_pnl {net_pnl:.2f} not within fee tolerance (diff={diff:.2f})")

    rows = _load_csv(pathlib.Path(args.fills_csv)) if args.fills_csv else []
    if not rows:
        runtime_min = float(report.get("execution", {}).get("runtime_sec", 0) or 0) / 60.0
        if runtime_min > args.warn_zero_fills_after_min:
            print(f"WARNING: 0 fills after {runtime_min:.1f} min runtime — consider using --fill-model probabilistic")
        print("PASS: acceptance checks succeeded (0 fills)")
        return

    per_market_shares: Dict[str, float] = {}
    per_market_price: Dict[str, float] = {}
    total_exposure = 0.0
    for row in rows:
        side = (row.get("side") or "").lower()
        market = row.get("market_id") or "unknown"
        shares = float(row.get("filled_shares") or 0.0)
        price = float(row.get("fill_price") or 0.0)
        per_market_price[market] = price
        delta_shares = shares if side == "buy" else -shares
        per_market_shares[market] = per_market_shares.get(market, 0.0) + delta_shares
        if per_market_shares[market] < -1e-9:
            _fail(f"negative inventory for market {market}")

        per_market_usd = {
            m: max(0.0, per_market_shares[m]) * per_market_price.get(m, 0.0)
            for m in per_market_shares
        }
        total_exposure = sum(per_market_usd.values())
        if total_exposure > args.max_exposure_usd + 1e-6:
            _fail(f"total exposure exceeded: {total_exposure:.2f} > {args.max_exposure_usd:.2f}")
        if per_market_usd.get(market, 0.0) > args.max_per_market_exposure_usd + 1e-6:
            _fail(
                f"per-market exposure exceeded: {market} {per_market_usd.get(market, 0.0):.2f} "
                f"> {args.max_per_market_exposure_usd:.2f}"
            )

    print("PASS: acceptance checks succeeded")


if __name__ == "__main__":
    main()
