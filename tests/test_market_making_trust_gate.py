import sqlite3
from datetime import datetime

import pytest

from database import Database
from execution_diagnostics import ExecutionDiagnostics
from market_making_trust import run_trust_gates


def test_trust_gate_rejects_invalid_fill_snapshot(tmp_path):
    db_path = tmp_path / "mm.db"
    db = Database(str(db_path))
    db.initialize_portfolio(1000.0)

    diagnostics = ExecutionDiagnostics(db_path=str(db_path))
    # Insert invalid execution record (best_bid > best_ask) directly.
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """INSERT INTO execution_records
               (run_id, run_tag, order_id, market_id, side, order_type, qty_shares,
                whale_ref_type, order_sent_ts, fill_ts,
                best_bid, best_ask, mid_price, fill_price, fill_price_source,
                created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "run-1",
                "MM_TEST",
                "ord-1",
                "mkt-1",
                "buy",
                "limit",
                10.0,
                "unknown",
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
                0.6,
                0.5,
                0.55,
                0.55,
                "fill",
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()

    with pytest.raises(RuntimeError):
        run_trust_gates(
            stage="pre_run",
            db=db,
            diagnostics=diagnostics,
            run_id="run-1",
            run_tag="MM_TEST",
            eps=1e-6,
            output_dir=str(tmp_path),
            run_meta={"note": "test"},
        )
