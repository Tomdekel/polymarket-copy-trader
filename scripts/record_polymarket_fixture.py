#!/usr/bin/env python3
"""Record Polymarket snapshots into offline fixtures."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time
from datetime import UTC, datetime
from typing import Any, Dict, List

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from data_provider import PolymarketAPIProvider
from measurement_mode import MeasurementSelector


def _is_binary_market(market: Dict[str, Any]) -> bool:
    outcomes = market.get("outcomes")
    if isinstance(outcomes, list) and len(outcomes) == 2:
        return True
    tokens = market.get("tokens")
    if isinstance(tokens, list) and len(tokens) == 2:
        return True
    outcome_prices = market.get("outcomePrices")
    if isinstance(outcome_prices, list) and len(outcome_prices) == 2:
        return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Record Polymarket fixture snapshots.")
    parser.add_argument("--out", required=True, help="Output fixture directory")
    parser.add_argument("--markets", type=int, default=10, help="Number of markets to record")
    parser.add_argument("--minutes", type=int, default=30, help="Minutes to record")
    parser.add_argument("--run-tag", default="RECORDING_TEST", help="Run tag")
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out)
    snapshots_dir = out_dir / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    provider = PolymarketAPIProvider()
    selector = MeasurementSelector()

    markets = provider.get_markets()
    candidates: List[str] = []
    market_map: Dict[str, Dict[str, Any]] = {}
    for market in markets:
        if not _is_binary_market(market):
            continue
        market_id = market.get("conditionId") or market.get("id")
        if not market_id:
            continue
        snapshot = provider.get_snapshot(market_id)
        tier = selector.tier_for_snapshot(snapshot.spread_pct, (snapshot.depth_bid_1 or 0) + (snapshot.depth_ask_1 or 0))
        if tier != "A":
            continue
        candidates.append(str(market_id))
        market_map[str(market_id)] = market
    candidates.sort()
    selected = candidates[: args.markets]

    markets_payload = [market_map[m] for m in selected]
    (out_dir / "markets.json").write_text(json.dumps(markets_payload, indent=2), encoding="utf-8")

    start = time.time()
    while time.time() - start < args.minutes * 60:
        for market_id in selected:
            snap = provider.get_snapshot(market_id)
            row = {
                "timestamp": datetime.now(UTC).isoformat(),
                "best_bid": snap.best_bid,
                "best_ask": snap.best_ask,
                "mid_price": snap.mid_price,
                "depth_bid_1": snap.depth_bid_1,
                "depth_ask_1": snap.depth_ask_1,
                "last_trade_price": snap.last_trade_price,
            }
            with (snapshots_dir / f"{market_id}.jsonl").open("a", encoding="utf-8") as f:
                f.write(json.dumps(row) + "\n")
        time.sleep(2)

    print(f"Wrote fixture to {out_dir}")


if __name__ == "__main__":
    main()
