import json
import subprocess
import sys
from datetime import datetime

from database import Database
from execution_diagnostics import ExecutionDiagnostics


def test_analyzer_writes_trust_section(tmp_path):
    db_path = tmp_path / "mm_analyze.db"
    db = Database(str(db_path))
    db.initialize_portfolio(1000.0)

    run_id = "run-test"
    run_tag = "MM_TEST"

    # Insert a trade row tied to run_id/run_tag.
    db.add_trade(
        market="mkt-1",
        side="BUY",
        size=10.0,
        price=0.5,
        target_wallet="market_making",
        market_slug="mkt-1",
        outcome="YES",
        entry_price_source="fill",
        current_price_source="fill",
        run_id=run_id,
        run_tag=run_tag,
    )

    diagnostics = ExecutionDiagnostics(db_path=str(db_path), live_mode=True)
    payload = {
        "run_id": run_id,
        "run_tag": run_tag,
        "order_id": "ord-1",
        "trade_id": 1,
        "market_id": "mkt-1",
        "market_slug": "mkt-1",
        "side": "buy",
        "order_type": "limit",
        "qty_shares": 20.0,
        "intended_limit_price": 0.5,
        "time_in_force": "GTC",
        "whale_signal_ts": None,
        "whale_entry_ref_price": None,
        "whale_ref_type": "unknown",
        "our_decision_ts": datetime.utcnow(),
        "order_sent_ts": datetime.utcnow(),
        "exchange_ack_ts": datetime.utcnow(),
        "fill_ts": datetime.utcnow(),
        "best_bid": 0.49,
        "best_ask": 0.51,
        "mid_price": 0.50,
        "depth_bid_1": 1000,
        "depth_ask_1": 1000,
        "depth_bid_2": None,
        "depth_ask_2": None,
        "last_trade_price": 0.50,
        "fill_price": 0.50,
        "entry_price_source": "fill",
        "current_price_source": "fill",
        "exit_price_source": "unknown",
        "fill_price_source": "fill",
        "filled_shares": 20.0,
        "fees_usd": 0.01,
        "is_partial_fill": False,
        "fill_count": 1,
    }
    diagnostics.record_fill(payload)

    # Add non-truthful rows for exclusion counts.
    non_fill = dict(payload)
    non_fill.update(
        {
            "order_id": "ord-2",
            "fill_ts": None,
            "fill_price": None,
            "fill_price_source": "unknown",
        }
    )
    diagnostics.record_order_sent(non_fill)

    bad_source = dict(payload)
    bad_source.update(
        {
            "order_id": "ord-3",
            "fill_price_source": "quote",
        }
    )
    diagnostics.record_fill(bad_source)

    missing_snapshot = dict(payload)
    missing_snapshot.update(
        {
            "order_id": "ord-4",
            "best_bid": None,
            "best_ask": None,
            "mid_price": None,
        }
    )
    diagnostics.record_fill(missing_snapshot)

    out_json = tmp_path / "report.json"
    out_md = tmp_path / "report.md"
    subprocess.run(
        [
            sys.executable,
            "scripts/analyze_market_making.py",
            "--db",
            str(db_path),
            "--run-tag",
            run_tag,
            "--run-id",
            run_id,
            "--output-json",
            str(out_json),
            "--output-md",
            str(out_md),
        ],
        check=True,
    )

    report = json.loads(out_json.read_text(encoding="utf-8"))
    md = out_md.read_text(encoding="utf-8")
    assert "truthful_rate" in report["truth"]
    assert report["truth"]["exclusions"]["not_filled"] == 1
    assert report["truth"]["exclusions"]["non_fill_source"] == 1
    assert report["truth"]["exclusions"]["missing_snapshot"] == 1
    assert "## Trust" in md
