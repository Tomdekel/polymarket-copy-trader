"""Tests for measurement-mode helpers."""

from datetime import datetime
from types import SimpleNamespace

from measurement_mode import MeasurementSelector, build_synthetic_baseline


def test_synthetic_baseline_populated():
    ts = datetime(2026, 2, 5, 12, 0, 0)
    baseline = build_synthetic_baseline(ts, 0.42)
    assert baseline["whale_ref_type"] == "synthetic"
    assert baseline["whale_signal_ts"] == ts
    assert baseline["whale_entry_ref_price"] == 0.42


def test_selector_produces_multiple_tiers_deterministically():
    selector = MeasurementSelector()
    positions = [
        SimpleNamespace(market="a", market_slug="A", outcome="YES"),
        SimpleNamespace(market="b", market_slug="B", outcome="YES"),
        SimpleNamespace(market="c", market_slug="C", outcome="YES"),
    ]
    snapshots = {
        "a": {"best_bid": 0.499, "best_ask": 0.501, "mid_price": 0.5, "depth_bid_1": 700, "depth_ask_1": 600},
        "b": {"best_bid": 0.495, "best_ask": 0.505, "mid_price": 0.5, "depth_bid_1": 150, "depth_ask_1": 150},
        "c": {"best_bid": 0.40, "best_ask": 0.50, "mid_price": 0.45, "depth_bid_1": 50, "depth_ask_1": 30},
    }

    candidates = selector.build_candidates(positions, snapshots)
    tiers = {c.market_id: c.tier for c in candidates}
    assert tiers == {"a": "A", "b": "B", "c": "C"}

    cycle = selector.select_cycle(candidates, n=6)
    assert [c.tier for c in cycle] == ["A", "B", "C", "A", "B", "C"]
