#!/usr/bin/env python3
"""Offline smoke test for market making pipeline using sample fixtures."""

from __future__ import annotations

import pathlib
import sqlite3
import subprocess
import sys


def main() -> None:
    repo = pathlib.Path(__file__).resolve().parents[1]
    run_tag = "OFFLINE_SMOKE"
    run_cmd = [
        sys.executable,
        "scripts/run_market_making_experiment.py",
        "--data-mode",
        "offline",
        "--fixture-dir",
        "fixtures/market_making/sample",
        "--run-tag",
        run_tag,
        "--bankroll",
        "10000",
        "--markets",
        "2",
        "--quote-size-usd",
        "10",
        "--k-ticks",
        "0",
        "--max-runtime-min",
        "1",
        "--max-exposure-usd",
        "5000",
        "--max-per-market-exposure-usd",
        "500",
    ]
    subprocess.run(run_cmd, check=True, cwd=repo)

    run_id = None
    db_path = repo / "trades.db"
    if db_path.exists():
        with sqlite3.connect(str(db_path)) as conn:
            row = conn.execute(
                "SELECT run_id FROM execution_records WHERE run_tag = ? ORDER BY updated_at DESC LIMIT 1",
                (run_tag,),
            ).fetchone()
            if row:
                run_id = row[0]
    if not run_id:
        raise SystemExit("Failed to resolve run_id for offline smoke")

    analyze_cmd = [
        sys.executable,
        "scripts/analyze_market_making.py",
        "--db",
        "trades.db",
        "--run-tag",
        run_tag,
        "--run-id",
        run_id,
        "--output-json",
        f"reports/market_making_{run_tag}.json",
        "--output-md",
        f"reports/market_making_{run_tag}.md",
        "--export-csv",
        f"reports/market_making_{run_tag}_fills.csv",
    ]
    subprocess.run(analyze_cmd, check=True, cwd=repo)

    check_cmd = [
        sys.executable,
        "scripts/check_market_making_run.py",
        "--report-json",
        f"reports/market_making_{run_tag}.json",
        "--fills-csv",
        f"reports/market_making_{run_tag}_fills.csv",
        "--truthful-rate-min",
        "0.99",
        "--max-exposure-usd",
        "5000",
        "--max-per-market-exposure-usd",
        "500",
        "--baseline",
    ]
    subprocess.run(check_cmd, check=True, cwd=repo)


if __name__ == "__main__":
    main()
