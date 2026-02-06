#!/usr/bin/env python3
"""Calibrate probabilistic fill model parameters against offline fixtures.

Runs the fill model over fixture snapshots with various parameter
combinations and reports expected fill rates per configuration.  This
helps choose alpha/base_liquidity/p_max before a live run.

Usage:
    python scripts/calibrate_fill_model.py \
        --fixture-dir fixtures/market_making/sample \
        --k-ticks 2

Outputs a table of (alpha, base_liquidity, p_max) -> expected fill rate.
"""
from __future__ import annotations

import argparse
import itertools
import pathlib
import sys
from datetime import UTC, datetime
from typing import List, Tuple

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from data_provider import FixtureProvider
from fill_model import ProbabilisticFillModel
from market_making_strategy import MarketSnapshot, compute_mid, quote_prices


def _run_sweep(
    provider: FixtureProvider,
    market_ids: List[str],
    *,
    k_ticks: int,
    tick_size: float,
    alpha: float,
    base_liquidity: float,
    p_max: float,
    seed: int,
) -> Tuple[int, int]:
    """Return (total_evaluations, total_fills) for one parameter combo."""
    model = ProbabilisticFillModel(
        tick_size=tick_size,
        alpha=alpha,
        base_liquidity=base_liquidity,
        p_max=p_max,
        seed=seed,
    )
    evals = 0
    fills = 0
    now = datetime.now(UTC)

    for market_id in market_ids:
        provider.reset_market(market_id)
        num_snapshots = provider.snapshot_count(market_id)
        for _ in range(num_snapshots):
            snap = provider.get_snapshot(market_id, outcome="YES", advance=True)
            mid = compute_mid(snap.best_bid, snap.best_ask, snap.mid_price)
            if mid is None:
                continue
            bid_price, ask_price = quote_prices(mid, tick_size, k_ticks)
            if bid_price is not None:
                decision = model.should_fill("buy", bid_price, snap, now)
                evals += 1
                if decision.fill:
                    fills += 1
            if ask_price is not None:
                decision = model.should_fill("sell", ask_price, snap, now)
                evals += 1
                if decision.fill:
                    fills += 1

    return evals, fills


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate probabilistic fill model")
    parser.add_argument("--fixture-dir", required=True, help="Path to fixture directory")
    parser.add_argument("--fixture-profile", default="default", help="Fixture profile")
    parser.add_argument("--k-ticks", type=int, default=2, help="Ticks away from mid")
    parser.add_argument("--tick-size", type=float, default=0.01, help="Tick size")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    provider = FixtureProvider(pathlib.Path(args.fixture_dir), profile=args.fixture_profile)
    markets_data = provider.get_markets()
    market_ids = [m.get("conditionId") or m.get("id") or m.get("market_id") for m in markets_data]
    market_ids = [m for m in market_ids if m]

    if not market_ids:
        raise SystemExit("No markets found in fixtures")

    alphas = [0.5, 1.0, 1.5, 2.0, 3.0]
    base_liquidities = [0.05, 0.10, 0.15, 0.20]
    p_maxes = [0.10, 0.20, 0.30]

    print(f"Fixture: {args.fixture_dir} | Markets: {len(market_ids)} | k={args.k_ticks}")
    print(f"{'alpha':>6} {'base_liq':>9} {'p_max':>6} {'evals':>7} {'fills':>6} {'rate':>8}")
    print("-" * 50)

    for alpha, base_liq, p_max in itertools.product(alphas, base_liquidities, p_maxes):
        evals, fills = _run_sweep(
            provider,
            market_ids,
            k_ticks=args.k_ticks,
            tick_size=args.tick_size,
            alpha=alpha,
            base_liquidity=base_liq,
            p_max=p_max,
            seed=args.seed,
        )
        rate = fills / evals if evals > 0 else 0.0
        print(f"{alpha:6.1f} {base_liq:9.2f} {p_max:6.2f} {evals:7d} {fills:6d} {rate:8.4f}")


if __name__ == "__main__":
    main()
