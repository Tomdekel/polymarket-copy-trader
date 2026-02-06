#!/usr/bin/env python3
"""Analyze market making experiment outputs and emit reports."""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
from collections import defaultdict
from datetime import datetime
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from config_loader import load_config
from database import Database
from execution_diagnostics import ExecutionDiagnostics
from market_making_rewards import RewardLedger


def _to_float(value: Any) -> Optional[float]:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _quantile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    sorted_vals = sorted(values)
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def _summary(values: Sequence[float]) -> Dict[str, Optional[float]]:
    return {
        "count": float(len(values)),
        "mean": mean(values) if values else None,
        "p50": _quantile(values, 0.50),
        "p90": _quantile(values, 0.90),
        "p95": _quantile(values, 0.95),
    }


def _truthful_filter(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    truthful: List[Dict[str, Any]] = []
    exclusions = {"not_filled": 0, "non_fill_source": 0, "missing_snapshot": 0}
    for row in rows:
        if row.get("fill_price") is None:
            exclusions["not_filled"] += 1
            continue
        if row.get("fill_price_source") != "fill":
            exclusions["non_fill_source"] += 1
            continue
        if row.get("mid_price") is None or row.get("best_bid") is None or row.get("best_ask") is None:
            exclusions["missing_snapshot"] += 1
            continue
        truthful.append(row)
    return truthful, exclusions


def _inventory_events(trades: List[Dict[str, Any]]) -> List[Tuple[datetime, str, float]]:
    events: List[Tuple[datetime, str, float]] = []
    for trade in trades:
        ts = _parse_dt(trade.get("timestamp"))
        if not ts:
            continue
        side = (trade.get("side") or "BUY").upper()
        size = float(trade.get("size") or 0.0)
        if side == "BUY":
            events.append((ts, trade.get("market") or "unknown", size))
        else:
            events.append((ts, trade.get("market") or "unknown", -size))

        closed_at = _parse_dt(trade.get("closed_at"))
        if closed_at:
            if side == "BUY":
                events.append((closed_at, trade.get("market") or "unknown", -size))
            else:
                events.append((closed_at, trade.get("market") or "unknown", size))
    events.sort(key=lambda e: e[0])
    return events


def _time_weighted_avg(events: List[Tuple[datetime, float]]) -> float:
    if len(events) < 2:
        return abs(events[0][1]) if events else 0.0
    total = 0.0
    total_time = 0.0
    for (t0, v0), (t1, _) in zip(events, events[1:]):
        dt = (t1 - t0).total_seconds()
        if dt <= 0:
            continue
        total += abs(v0) * dt
        total_time += dt
    return total / total_time if total_time > 0 else 0.0


def _compute_equity_curve(
    fills: List[Dict[str, Any]],
    bankroll: float,
) -> Tuple[List[Tuple[datetime, float]], Optional[float]]:
    cash = bankroll
    positions: Dict[str, float] = {}
    last_mid: Dict[str, float] = {}
    equity_series: List[Tuple[datetime, float]] = []

    for row in sorted(fills, key=lambda r: r.get("fill_ts") or ""):
        fill_ts = _parse_dt(row.get("fill_ts"))
        if not fill_ts:
            continue
        market = row.get("market_id") or "unknown"
        side = (row.get("side") or "").lower()
        qty = float(row.get("filled_shares") or 0.0)
        fill_price = float(row.get("fill_price") or 0.0)
        mid = _to_float(row.get("mid_price")) or fill_price
        last_mid[market] = mid

        if side == "buy":
            cash -= qty * fill_price
            positions[market] = positions.get(market, 0.0) + qty
        elif side == "sell":
            cash += qty * fill_price
            positions[market] = max(0.0, positions.get(market, 0.0) - qty)

        equity = cash
        for mkt, shares in positions.items():
            equity += shares * (last_mid.get(mkt, 0.0))
        equity_series.append((fill_ts, equity))

    max_dd = None
    if equity_series:
        peak = equity_series[0][1]
        max_dd_val = 0.0
        for _, value in equity_series:
            if value > peak:
                peak = value
            dd = (peak - value) / peak if peak > 0 else 0.0
            max_dd_val = max(max_dd_val, dd)
        max_dd = max_dd_val
    return equity_series, max_dd


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze market making experiment.")
    parser.add_argument("--db", default="trades.db", help="SQLite DB path")
    parser.add_argument("--run-tag", required=True, help="Run tag to analyze")
    parser.add_argument("--run-id", default=None, help="Optional run_id override")
    parser.add_argument("--output-json", default=None, help="JSON output path")
    parser.add_argument("--output-md", default=None, help="Markdown output path")
    parser.add_argument("--export-csv", default=None, help="Optional CSV export path")
    args = parser.parse_args()

    cfg = load_config()
    mm_cfg = cfg.get("market_making", {})
    max_hold_time_sec = int(mm_cfg.get("max_hold_time_sec", 14400))
    bankroll = float(mm_cfg.get("bankroll", 10000.0)) if "bankroll" in mm_cfg else 10000.0

    db = Database(args.db)
    diagnostics = ExecutionDiagnostics(db_path=args.db)
    rewards = RewardLedger(db_path=args.db)

    rows = diagnostics.fetch_records(run_tag=args.run_tag, run_id=args.run_id)
    if not rows:
        print("No execution records found for run tag.")
        return

    run_id = args.run_id or rows[0].get("run_id") or "unknown"
    all_rows = [r for r in rows if r.get("run_id") == run_id]

    truthful_rows, exclusions = _truthful_filter(all_rows)
    total_rows = len(all_rows)
    truthful_rate = (len(truthful_rows) / total_rows * 100.0) if total_rows else 0.0

    if args.export_csv:
        diagnostics.export_slippage_csv(
            output_path=args.export_csv,
            run_id=run_id,
            run_tag=args.run_tag,
            only_filled=True,
            require_fill_source=True,
            require_snapshot=True,
        )

    fills = truthful_rows
    passive_fills = sum(1 for r in fills if r.get("spread_crossed") is False)
    passive_fill_rate = (passive_fills / len(fills) * 100.0) if fills else 0.0
    spread_crossed = sum(1 for r in fills if r.get("spread_crossed") is True)
    spread_crossed_rate = (spread_crossed / len(fills) * 100.0) if fills else 0.0

    quote_slippage = [v for v in (_to_float(r.get("quote_slippage_pct")) for r in fills) if v is not None]
    time_to_fill = []
    for r in fills:
        sent = _parse_dt(r.get("order_sent_ts"))
        filled = _parse_dt(r.get("fill_ts"))
        if sent and filled:
            time_to_fill.append((filled - sent).total_seconds())

    trades = [t for t in db.get_all_trades() if (t.get("run_id") or "") == run_id and (t.get("run_tag") or "") == args.run_tag]
    stats = db.get_portfolio_stats()
    if stats.get("initial_budget"):
        bankroll = float(stats.get("initial_budget") or bankroll)
    recon = db.reconcile_portfolio()
    gross_pnl_usd = float(recon.get("total_realized") or 0.0) + float(recon.get("total_unrealized") or 0.0)
    ledger_pnl = sum(float(t.get("realized_pnl") or 0.0) + float(t.get("unrealized_pnl") or 0.0) for t in trades)
    fees_usd = sum(float(r.get("fees_usd") or 0.0) for r in fills)
    reward_rows = rewards.get_rewards(run_id=run_id, run_tag=args.run_tag)
    rewards_usd = sum(float(r.get("reward_usd") or 0.0) for r in reward_rows)
    reward_sources = sorted({r.get("reward_source") for r in reward_rows if r.get("reward_source")})
    net_pnl_usd = gross_pnl_usd - fees_usd + rewards_usd
    net_without_rewards = gross_pnl_usd - fees_usd

    events = _inventory_events(trades)
    per_market_series: Dict[str, List[Tuple[datetime, float]]] = defaultdict(list)
    total_series: List[Tuple[datetime, float]] = []
    running_total = 0.0
    running_market: Dict[str, float] = defaultdict(float)
    for ts, market, delta in events:
        running_market[market] += delta
        running_total += delta
        per_market_series[market].append((ts, running_market[market]))
        total_series.append((ts, running_total))

    avg_locked = _time_weighted_avg(total_series)
    capital_utilization_pct = (avg_locked / bankroll * 100.0) if bankroll else 0.0
    inventory_values = [v for _, v in total_series]
    inventory_variance = 0.0
    if len(inventory_values) >= 2:
        mean_val = mean(inventory_values)
        inventory_variance = mean([(v - mean_val) ** 2 for v in inventory_values])

    max_inventory_by_market = {
        m: max(abs(v) for _, v in series) if series else 0.0 for m, series in per_market_series.items()
    }

    inventory_drawdowns: Dict[str, float] = {}
    for market, series in per_market_series.items():
        if not series:
            continue
        peak = abs(series[0][1])
        max_dd = 0.0
        for _, value in series:
            value_abs = abs(value)
            if value_abs > peak:
                peak = value_abs
            dd = peak - value_abs
            max_dd = max(max_dd, dd)
        inventory_drawdowns[market] = max_dd

    open_positions = [t for t in trades if (t.get("status") or "open") == "open"]
    now = datetime.utcnow()
    stuck_inventory = 0
    for pos in open_positions:
        ts = _parse_dt(pos.get("timestamp"))
        if not ts:
            continue
        age = (now - ts).total_seconds()
        if age > max_hold_time_sec:
            stuck_inventory += 1

    hold_times = []
    for trade in trades:
        if (trade.get("status") or "open").lower() != "closed":
            continue
        opened = _parse_dt(trade.get("timestamp"))
        closed = _parse_dt(trade.get("closed_at"))
        if opened and closed:
            hold_times.append((closed - opened).total_seconds())

    equity_curve, max_drawdown_pct = _compute_equity_curve(fills, bankroll)
    equity_jump_warning = False
    if len(equity_curve) >= 2:
        for (t0, e0), (t1, e1), row in zip(equity_curve, equity_curve[1:], fills[1:]):
            trade_val = abs(float(row.get("filled_shares") or 0.0) * float(row.get("fill_price") or 0.0))
            if abs(e1 - e0) > trade_val * 2 + 1:
                equity_jump_warning = True
                break

    markets_table: List[Dict[str, Any]] = []
    per_market_trades: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        per_market_trades[trade.get("market") or "unknown"].append(trade)
    per_market_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in fills:
        per_market_rows[row.get("market_id") or "unknown"].append(row)

    for market, rows in per_market_rows.items():
        spreads = [v for v in (_to_float(r.get("spread_pct")) for r in rows) if v is not None]
        depths = []
        for r in rows:
            b = _to_float(r.get("depth_bid_1"))
            a = _to_float(r.get("depth_ask_1"))
            if b is None and a is None:
                continue
            depths.append((b or 0.0) + (a or 0.0))
        slippage_vals = [abs(v) for v in (_to_float(r.get("quote_slippage_pct")) for r in rows) if v is not None]
        trade_rows = per_market_trades.get(market, [])
        realized = sum(float(t.get("realized_pnl") or 0.0) for t in trade_rows)
        unrealized = sum(float(t.get("unrealized_pnl") or 0.0) for t in trade_rows)
        total_pnl = realized + unrealized
        markets_table.append(
            {
                "market": market,
                "net_pnl_usd": total_pnl,
                "fill_rate": len(rows),
                "avg_spread_pct": mean(spreads) if spreads else None,
                "avg_depth": mean(depths) if depths else None,
                "max_inventory_held": max_inventory_by_market.get(market, 0.0),
                "worst_tail_slippage": _quantile(slippage_vals, 0.95),
            }
        )

    markets_table.sort(key=lambda x: float(x.get("net_pnl_usd") or 0.0), reverse=True)

    integrity_issues = db.validate_trade_integrity(eps=1e-6)
    reconciliation_status = "pass" if not integrity_issues else "fail"

    summary = {
        "run_id": run_id,
        "run_tag": args.run_tag,
        "truth": {
            "truthful_rows": len(truthful_rows),
            "total_rows": total_rows,
            "truthful_rate": truthful_rate,
            "exclusions": exclusions,
            "reconciliation_status": reconciliation_status,
            "reconciliation_eps": 1e-6,
        },
        "pnl": {
            "gross_pnl_usd": gross_pnl_usd,
            "fees_usd": fees_usd,
            "rewards_usd": rewards_usd,
            "net_pnl_usd": net_pnl_usd,
            "net_without_rewards_usd": net_without_rewards,
            "reward_sources": reward_sources,
        },
        "execution": {
            "passive_fill_rate": passive_fill_rate,
            "spread_crossed_rate": spread_crossed_rate,
            "quote_slippage_pct": _summary(quote_slippage),
            "time_to_fill_sec": _summary(time_to_fill),
        },
        "inventory": {
            "avg_locked_usd": avg_locked,
            "capital_utilization_pct": capital_utilization_pct,
            "inventory_turnover": (sum(abs(float(r.get("filled_shares") or 0.0) * float(r.get("fill_price") or 0.0)) for r in fills)
                                  / avg_locked) if avg_locked else None,
            "stuck_inventory_count": stuck_inventory,
            "time_in_inventory_sec": _summary(hold_times),
            "max_inventory_drawdown_by_market": inventory_drawdowns,
            "max_inventory_held_by_market": max_inventory_by_market,
            "max_drawdown_pct": max_drawdown_pct,
            "inventory_variance": inventory_variance,
        },
        "markets_top10": markets_table[:10],
        "warnings": [],
    }

    if spread_crossed_rate > 5:
        summary["warnings"].append("not passive market making: spread_crossed_rate > 5%")
    if passive_fill_rate < 70:
        summary["warnings"].append("mostly taking liquidity: passive_fill_rate < 70%")
    if truthful_rate < 80:
        summary["warnings"].append("dataset too thin; rerun")
    if rewards_usd == 0 and net_without_rewards < 0:
        summary["warnings"].append("net_pnl_without_rewards negative and rewards unknown; unproven")
    if equity_jump_warning:
        summary["warnings"].append("equity curve jump exceeds expected trade value; check reconciliation")
    if abs(ledger_pnl - gross_pnl_usd) > 1e-6:
        summary["warnings"].append("reported P&L does not match ledger reconciliation")
    if reconciliation_status == "fail":
        summary["warnings"].append("reconciliation gate failed; report may be invalid")

    out_json = args.output_json or f"reports/market_making_{args.run_tag}.json"
    out_md = args.output_md or f"reports/market_making_{args.run_tag}.md"
    pathlib.Path(out_json).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(out_json).write_text(json.dumps(summary, indent=2), encoding="utf-8")

    md_lines = [
        f"# Market Making Report: {args.run_tag}",
        "",
        "## Trust",
        f"- truthful_rows: {summary['truth']['truthful_rows']}",
        f"- total_rows: {summary['truth']['total_rows']}",
        f"- truthful_rate: {summary['truth']['truthful_rate']:.2f}%",
        f"- exclusions: {summary['truth']['exclusions']}",
        f"- reconciliation_status: {reconciliation_status} (eps=1e-6)",
        "",
        "## P&L Breakdown",
        f"- gross_pnl_usd: {gross_pnl_usd:.2f}",
        f"- fees_usd: {fees_usd:.2f}",
        f"- rewards_usd: {rewards_usd:.2f}",
        f"- reward_sources: {reward_sources}",
        f"- net_pnl_usd: {net_pnl_usd:.2f}",
        f"- net_without_rewards_usd: {net_without_rewards:.2f}",
        "",
        "## Execution Quality",
        f"- passive_fill_rate: {passive_fill_rate:.2f}%",
        f"- spread_crossed_rate: {spread_crossed_rate:.2f}%",
        f"- quote_slippage_p50: {_summary(quote_slippage)['p50']}",
        f"- quote_slippage_p90: {_summary(quote_slippage)['p90']}",
        f"- quote_slippage_p95: {_summary(quote_slippage)['p95']}",
        "",
        "## Inventory Risk",
        f"- max_drawdown_pct: {(max_drawdown_pct or 0.0) * 100:.2f}%",
        f"- inventory_variance: {inventory_variance:.2f}",
        f"- stuck_inventory_count: {stuck_inventory}",
        "",
        "## Top 10 Markets",
        "| market | net pnl | fill rate | avg spread % | avg depth | max inventory | worst tail slippage |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in markets_table[:10]:
        md_lines.append(
            f"| {row['market']} | {row['net_pnl_usd']:.2f} | {row['fill_rate']} | "
            f"{row['avg_spread_pct'] if row['avg_spread_pct'] is not None else '-'} | "
            f"{row['avg_depth'] if row['avg_depth'] is not None else '-'} | "
            f"{row['max_inventory_held']:.2f} | {row['worst_tail_slippage'] if row['worst_tail_slippage'] is not None else '-'} |"
        )

    if summary["warnings"]:
        md_lines.extend(["", "## Warnings"])
        md_lines.extend([f"- {w}" for w in summary["warnings"]])

    pathlib.Path(out_md).write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"Wrote {out_json}")
    print(f"Wrote {out_md}")


if __name__ == "__main__":
    main()
