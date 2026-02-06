#!/usr/bin/env python3
"""Generate a minimal static dashboard from market making JSON reports."""

from __future__ import annotations

import argparse
import csv
import json
import math
import pathlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


def _load_json(path: pathlib.Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_csv(path: pathlib.Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _compute_timeseries(rows: List[Dict[str, str]], bankroll: float) -> Dict[str, List[Tuple[datetime, float]]]:
    cash = bankroll
    positions: Dict[str, float] = {}
    last_mid: Dict[str, float] = {}
    equity: List[Tuple[datetime, float]] = []
    exposure: List[Tuple[datetime, float]] = []

    for row in sorted(rows, key=lambda r: r.get("fill_ts") or ""):
        ts = _parse_dt(row.get("fill_ts"))
        if not ts:
            continue
        market = row.get("market_id") or "unknown"
        side = (row.get("side") or "").lower()
        shares = float(row.get("filled_shares") or 0.0)
        fill_price = float(row.get("fill_price") or 0.0)
        mid = float(row.get("mid_price") or fill_price)
        last_mid[market] = mid

        if side == "buy":
            cash -= shares * fill_price
            positions[market] = positions.get(market, 0.0) + shares
        elif side == "sell":
            cash += shares * fill_price
            positions[market] = max(0.0, positions.get(market, 0.0) - shares)

        equity_val = cash
        exposure_val = 0.0
        for mkt, qty in positions.items():
            equity_val += qty * last_mid.get(mkt, 0.0)
            exposure_val += max(0.0, qty * last_mid.get(mkt, 0.0))
        equity.append((ts, equity_val))
        exposure.append((ts, exposure_val))

    return {"equity": equity, "exposure": exposure}


def _svg_line(series: List[Tuple[datetime, float]], width: int, height: int, stroke: str) -> str:
    if len(series) < 2:
        return f'<text x="0" y="{height/2}" fill="#666">no data</text>'
    values = [v for _, v in series]
    min_v = min(values)
    max_v = max(values)
    span = max(max_v - min_v, 1e-9)
    points = []
    for idx, (_, v) in enumerate(series):
        x = idx / (len(series) - 1) * width
        y = height - ((v - min_v) / span * height)
        points.append(f"{x:.2f},{y:.2f}")
    return f'<polyline fill="none" stroke="{stroke}" stroke-width="2" points="{" ".join(points)}" />'


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate minimal MM dashboard HTML.")
    parser.add_argument("--reports", nargs="+", required=True, help="Report JSON files")
    parser.add_argument("--output", default="reports/mm_dashboard.html", help="Output HTML path")
    parser.add_argument("--fills-dir", default="reports", help="Directory where *_fills.csv live")
    args = parser.parse_args()

    runs: List[Dict[str, Any]] = []
    for report_path in args.reports:
        path = pathlib.Path(report_path)
        report = _load_json(path)
        run_tag = report.get("run_tag") or path.stem
        fills_path = pathlib.Path(args.fills_dir) / f"market_making_{run_tag}_fills.csv"
        rows = _load_csv(fills_path)
        bankroll = 10000.0
        if "bankroll" in report:
            bankroll = float(report.get("bankroll") or bankroll)
        series = _compute_timeseries(rows, bankroll)
        runs.append({"report": report, "fills": rows, "series": series, "run_tag": run_tag})

    cards_html = []
    tables_html = []
    charts_html = []

    for run in runs:
        report = run["report"]
        pnl = report.get("pnl", {})
        execq = report.get("execution", {})
        inventory = report.get("inventory", {})
        run_tag = run["run_tag"]

        cards_html.append(
            f"""
            <div class="card">
              <h2>{run_tag}</h2>
              <div class="kpi-grid">
                <div><span>Net PnL</span><strong>{pnl.get("net_pnl_usd", 0):.2f}</strong></div>
                <div><span>Net w/o Rewards</span><strong>{pnl.get("net_without_rewards_usd", 0):.2f}</strong></div>
                <div><span>Fees</span><strong>{pnl.get("fees_usd", 0):.2f}</strong></div>
                <div><span>Rewards</span><strong>{pnl.get("rewards_usd", 0):.2f}</strong></div>
                <div><span>Max Drawdown %</span><strong>{(inventory.get("max_drawdown_pct") or 0) * 100:.2f}%</strong></div>
                <div><span>Stuck Inventory</span><strong>{inventory.get("stuck_inventory_count", 0)}</strong></div>
                <div><span>Passive Fill Rate</span><strong>{execq.get("passive_fill_rate", 0):.2f}%</strong></div>
                <div><span>Quote Slippage P90</span><strong>{execq.get("quote_slippage_pct", {}).get("p90")}</strong></div>
              </div>
            </div>
            """
        )

        rows = report.get("markets_top10", [])
        row_html = "\n".join(
            [
                f"<tr><td>{r.get('market')}</td><td>{r.get('net_pnl_usd', 0):.2f}</td>"
                f"<td>{r.get('worst_tail_slippage')}</td><td>{r.get('max_inventory_held', 0):.2f}</td></tr>"
                for r in rows
            ]
        )
        tables_html.append(
            f"""
            <div class="card">
              <h3>Worst Outcomes (Top 10)</h3>
              <table>
                <thead><tr><th>Market</th><th>Net PnL</th><th>Worst Tail Slippage</th><th>Max Inventory</th></tr></thead>
                <tbody>{row_html}</tbody>
              </table>
            </div>
            """
        )

        equity = run["series"]["equity"]
        exposure = run["series"]["exposure"]
        charts_html.append(
            f"""
            <div class="card">
              <h3>Time Series</h3>
              <div class="chart">
                <svg width="600" height="160">{_svg_line(equity, 600, 160, "#2563eb")}</svg>
                <div class="caption">Equity Curve</div>
              </div>
              <div class="chart">
                <svg width="600" height="160">{_svg_line(exposure, 600, 160, "#16a34a")}</svg>
                <div class="caption">Exposure</div>
              </div>
            </div>
            """
        )

    html = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <title>Market Making Dashboard</title>
      <style>
        body {{ font-family: Arial, sans-serif; background:#f6f7fb; margin:0; padding:24px; }}
        .card {{ background:white; border-radius:10px; padding:16px 20px; margin-bottom:20px; box-shadow:0 1px 4px rgba(0,0,0,0.06); }}
        .kpi-grid {{ display:grid; grid-template-columns: repeat(4, 1fr); gap:12px; }}
        .kpi-grid div span {{ display:block; font-size:12px; color:#666; }}
        .kpi-grid div strong {{ font-size:18px; }}
        table {{ width:100%; border-collapse:collapse; }}
        th, td {{ text-align:left; padding:8px; border-bottom:1px solid #eee; font-size:13px; }}
        h2, h3 {{ margin:0 0 12px 0; }}
        .chart {{ margin-bottom:12px; }}
        .caption {{ font-size:12px; color:#666; }}
      </style>
    </head>
    <body>
      {"".join(cards_html)}
      {"".join(tables_html)}
      {"".join(charts_html)}
    </body>
    </html>
    """
    out_path = pathlib.Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote dashboard: {out_path}")


if __name__ == "__main__":
    main()
