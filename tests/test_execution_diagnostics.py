"""Tests for slippage diagnostics schema and exporter."""

import csv
from datetime import datetime, timedelta

import pytest

from execution_diagnostics import ExecutionDiagnostics, OrderExecutionRecord, SLIPPAGE_EXPORT_COLUMNS


def test_order_execution_record_computes_derived_metrics_for_buy_cross():
    whale_ts = datetime(2026, 2, 1, 10, 0, 0)
    fill_ts = whale_ts + timedelta(seconds=5)

    record = OrderExecutionRecord(
        run_id="run-1",
        order_id="order-1",
        market_id="mkt-1",
        side="buy",
        order_type="limit",
        qty_shares=100,
        whale_signal_ts=whale_ts,
        whale_entry_ref_price=0.49,
        whale_ref_type="snapshot",
        order_sent_ts=whale_ts + timedelta(seconds=2),
        fill_ts=fill_ts,
        best_bid=0.49,
        best_ask=0.51,
        mid_price=0.50,
        fill_price=0.51,
        fill_price_source="fill",
        filled_shares=100,
        fill_count=1,
    )

    assert record.quote_slippage_pct == pytest.approx(0.02, rel=1e-9)
    assert record.half_spread_pct == pytest.approx(0.02, rel=1e-9)
    assert record.spread_crossed is True
    assert record.baseline_slippage_pct == pytest.approx((0.51 - 0.49) / 0.49, rel=1e-9)
    assert record.latency_ms == pytest.approx(5000.0, rel=1e-9)


def test_order_execution_record_spread_crossed_for_sell():
    record = OrderExecutionRecord(
        run_id="run-1",
        order_id="order-2",
        market_id="mkt-1",
        side="sell",
        order_type="market",
        qty_shares=50,
        best_bid=0.40,
        best_ask=0.42,
        mid_price=0.41,
        fill_price=0.40,
        fill_price_source="fill",
        filled_shares=50,
        fill_count=1,
    )

    assert record.spread_crossed is True
    assert record.impact_proxy_pct == pytest.approx(0.0, abs=1e-12)


def test_order_execution_record_rejects_invalid_price_range():
    with pytest.raises(ValueError, match=r"best_bid must be in \[0,1\]"):
        OrderExecutionRecord(
            run_id="run-1",
            order_id="order-3",
            market_id="mkt-1",
            side="buy",
            order_type="limit",
            qty_shares=10,
            best_bid=1.1,
            best_ask=1.2,
        )


def test_exporter_writes_required_columns(tmp_path):
    db_path = tmp_path / "slippage.db"
    csv_path = tmp_path / "slippage.csv"
    diagnostics = ExecutionDiagnostics(db_path=str(db_path), csv_path=str(csv_path))

    payload = {
        "run_id": "run-x",
        "order_id": "ord-1",
        "trade_id": 1,
        "market_id": "mkt-123",
        "market_slug": "slug",
        "side": "buy",
        "order_type": "limit",
        "qty_shares": 12.5,
        "intended_limit_price": 0.52,
        "time_in_force": "GTC",
        "whale_signal_ts": datetime(2026, 2, 1, 10, 0, 0),
        "whale_entry_ref_price": 0.5,
        "whale_ref_type": "avg_fill",
        "our_decision_ts": datetime(2026, 2, 1, 10, 0, 1),
        "order_sent_ts": datetime(2026, 2, 1, 10, 0, 2),
        "exchange_ack_ts": datetime(2026, 2, 1, 10, 0, 2),
        "fill_ts": datetime(2026, 2, 1, 10, 0, 3),
        "best_bid": 0.49,
        "best_ask": 0.51,
        "mid_price": 0.50,
        "spread_abs": 0.02,
        "spread_pct": 0.04,
        "depth_bid_1": 1000,
        "depth_ask_1": 900,
        "depth_bid_2": 800,
        "depth_ask_2": 700,
        "last_trade_price": 0.50,
        "fill_price": 0.51,
        "fill_price_source": "fill",
        "filled_shares": 12.5,
        "fees_usd": 0.01,
        "is_partial_fill": False,
        "fill_count": 1,
    }
    diagnostics.record_fill(payload)
    diagnostics.export_slippage_csv()

    with csv_path.open("r", encoding="utf-8") as f:
        header = f.readline().strip().split(",")

    assert header == SLIPPAGE_EXPORT_COLUMNS


def test_integration_record_order_and_fill_then_export_single_row(tmp_path):
    db_path = tmp_path / "integration.db"
    csv_path = tmp_path / "integration.csv"
    diagnostics = ExecutionDiagnostics(db_path=str(db_path), csv_path=str(csv_path))

    whale_ts = datetime(2026, 2, 1, 10, 0, 0)
    sent_ts = whale_ts + timedelta(seconds=1)
    fill_ts = whale_ts + timedelta(seconds=3)
    base = {
        "run_id": "run-int",
        "order_id": "ord-int-1",
        "market_id": "mkt-int-1",
        "market_slug": "int-slug",
        "side": "buy",
        "order_type": "limit",
        "qty_shares": 100.0,
        "intended_limit_price": 0.50,
        "time_in_force": "GTC",
        "whale_signal_ts": whale_ts,
        "whale_entry_ref_price": 0.49,
        "whale_ref_type": "avg_fill",
        "our_decision_ts": sent_ts,
        "order_sent_ts": sent_ts,
        "exchange_ack_ts": sent_ts,
        "best_bid": 0.49,
        "best_ask": 0.51,
        "mid_price": 0.50,
        "depth_bid_1": 1000.0,
        "depth_ask_1": 900.0,
        "last_trade_price": 0.50,
        "fees_usd": 0.0,
        "is_partial_fill": False,
        "fill_count": 0,
        "fill_ts": None,
        "fill_price": None,
        "fill_price_source": "placeholder",
        "filled_shares": None,
    }

    diagnostics.record_order_sent(base)

    fill_payload = dict(base)
    fill_payload.update(
        {
            "fill_ts": fill_ts,
            "fill_price": 0.51,
            "fill_price_source": "fill",
            "filled_shares": 100.0,
            "fill_count": 1,
        }
    )
    diagnostics.record_fill(fill_payload)
    diagnostics.export_slippage_csv()

    with csv_path.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    row = rows[0]
    assert row["order_id"] == "ord-int-1"
    assert float(row["quote_slippage_pct"]) == pytest.approx(0.02, rel=1e-9)
    assert float(row["latency_ms"]) == pytest.approx(3000.0, rel=1e-9)


def test_record_fill_before_order_sent_is_graceful(tmp_path):
    db_path = tmp_path / "integration_fill_first.db"
    diagnostics = ExecutionDiagnostics(db_path=str(db_path), csv_path=str(tmp_path / "fill_first.csv"))

    payload = {
        "run_id": "run-fill-first",
        "order_id": "ord-fill-first-1",
        "market_id": "mkt-fill-first",
        "side": "sell",
        "order_type": "market",
        "qty_shares": 10.0,
        "best_bid": 0.40,
        "best_ask": 0.42,
        "mid_price": 0.41,
        "fill_price": 0.40,
        "fill_price_source": "fill",
        "filled_shares": 10.0,
        "fill_count": 1,
    }
    fill_record = diagnostics.record_fill(payload)
    assert fill_record is not None
    recent = diagnostics.get_recent(limit=1, run_id="run-fill-first")
    assert len(recent) == 1
    assert recent[0]["order_id"] == "ord-fill-first-1"


def test_run_tag_propagates_into_execution_records(tmp_path):
    db_path = tmp_path / "run_tag.db"
    diagnostics = ExecutionDiagnostics(db_path=str(db_path), csv_path=str(tmp_path / "run_tag.csv"))
    payload = {
        "run_id": "run-tag-1",
        "run_tag": "LIQUID_FAST",
        "order_id": "ord-rt-1",
        "market_id": "mkt-rt-1",
        "side": "buy",
        "order_type": "limit",
        "qty_shares": 5.0,
        "best_bid": 0.49,
        "best_ask": 0.51,
        "mid_price": 0.5,
        "fill_price": 0.51,
        "filled_shares": 5.0,
        "fill_count": 1,
        "fill_price_source": "fill",
    }
    diagnostics.record_fill(payload)
    recent = diagnostics.get_recent(limit=1, run_tag="LIQUID_FAST")
    assert len(recent) == 1
    assert recent[0]["run_tag"] == "LIQUID_FAST"


def test_exporter_excludes_placeholder_and_missing_snapshot_rows(tmp_path):
    db_path = tmp_path / "exclusions.db"
    csv_path = tmp_path / "exclusions.csv"
    diagnostics = ExecutionDiagnostics(db_path=str(db_path), csv_path=str(csv_path))

    base = {
        "run_id": "run-filter",
        "market_id": "mkt-filter",
        "market_slug": "filter-slug",
        "side": "buy",
        "order_type": "limit",
        "qty_shares": 10.0,
    }

    diagnostics.record_fill(
        {
            **base,
            "order_id": "good-fill",
            "best_bid": 0.40,
            "best_ask": 0.42,
            "mid_price": 0.41,
            "fill_price": 0.42,
            "fill_price_source": "fill",
            "filled_shares": 10.0,
            "fill_count": 1,
        }
    )
    diagnostics.record_fill(
        {
            **base,
            "order_id": "placeholder-fill",
            "best_bid": 0.40,
            "best_ask": 0.42,
            "mid_price": 0.41,
            "fill_price": 0.42,
            "fill_price_source": "placeholder",
            "filled_shares": 10.0,
            "fill_count": 1,
        }
    )
    diagnostics.record_fill(
        {
            **base,
            "order_id": "missing-snapshot",
            "fill_price": 0.42,
            "fill_price_source": "fill",
            "filled_shares": 10.0,
            "fill_count": 1,
        }
    )
    diagnostics.record_order_sent(
        {
            **base,
            "order_id": "unfilled",
            "best_bid": 0.40,
            "best_ask": 0.42,
            "mid_price": 0.41,
            "fill_price": None,
            "fill_price_source": "unknown",
            "fill_count": 0,
        }
    )

    output, stats = diagnostics.export_slippage_csv(return_stats=True)
    with open(output, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    assert rows[0]["order_id"] == "good-fill"
    assert stats == {"not_filled": 1, "non_fill_source": 1, "missing_snapshot": 1}
