#!/usr/bin/env python3
"""Run a controlled market making experiment."""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from config_loader import load_config
from database import Database
from data_provider import FixtureProvider, PolymarketAPIProvider
from execution_diagnostics import ExecutionDiagnostics
from fill_model import DeterministicCrossingFillModel, ProbabilisticFillModel
from market_making_engine import MarketMakingEngine
from market_making_rewards import RewardLedger
from market_making_strategy import MarketSnapshot
from market_making_trust import run_trust_gates
from measurement_mode import MeasurementSelector


def _load_whitelist(path: pathlib.Path) -> List[str]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        return []
    return [str(x) for x in data if x]


def _parse_json_field(value: Any) -> Any:
    """Parse a field that may be a JSON-encoded string."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value


def _is_binary_market(market: Dict[str, Any]) -> bool:
    outcomes = _parse_json_field(market.get("outcomes"))
    if isinstance(outcomes, list) and len(outcomes) == 2:
        return True
    tokens = _parse_json_field(market.get("tokens"))
    if isinstance(tokens, list) and len(tokens) == 2:
        return True
    outcome_prices = _parse_json_field(market.get("outcomePrices"))
    if isinstance(outcome_prices, list) and len(outcome_prices) == 2:
        return True
    return False


def _snapshot_for_market(provider, market_id: str, advance: bool = True) -> MarketSnapshot:
    return provider.get_snapshot(market_id, outcome="YES", advance=advance)


def _select_markets(
    provider,
    *,
    whitelist: List[str],
    max_markets: int,
) -> List[str]:
    if whitelist:
        return whitelist[:max_markets] if max_markets else whitelist

    selector = MeasurementSelector()
    markets = provider.get_markets()
    candidates: List[str] = []
    for market in markets:
        if not _is_binary_market(market):
            continue
        market_id = market.get("conditionId") or market.get("id")
        if not market_id:
            continue
        snapshot = _snapshot_for_market(provider, market_id, advance=False)
        tier = selector.tier_for_snapshot(snapshot.spread_pct, (snapshot.depth_bid_1 or 0) + (snapshot.depth_ask_1 or 0))
        if tier != "A":
            continue
        candidates.append(str(market_id))
    candidates.sort()
    return candidates[:max_markets] if max_markets else candidates


def main() -> None:
    parser = argparse.ArgumentParser(description="Run market making experiment")
    parser.add_argument("--db", default="trades.db", help="SQLite DB path")
    parser.add_argument("--run-tag", required=True, help="Run tag for diagnostics")
    parser.add_argument("--bankroll", type=float, default=10000, help="Starting bankroll in USD")
    parser.add_argument("--markets", type=int, default=10, help="Number of markets to quote")
    parser.add_argument("--whitelist", default=None, help="Whitelist JSON path")
    parser.add_argument("--quote-size-usd", type=float, default=None, help="Quote size in USD")
    parser.add_argument("--k-ticks", type=int, default=None, help="Ticks away from mid")
    parser.add_argument("--max-runtime-min", type=int, default=30, help="Max runtime minutes")
    parser.add_argument("--max-exposure-usd", type=float, default=None, help="Max total exposure USD")
    parser.add_argument("--max-per-market-exposure-usd", type=float, default=None, help="Max per-market exposure USD")
    parser.add_argument("--max-fills", type=int, default=None, help="Optional max fill count")
    parser.add_argument("--data-mode", choices=["online", "offline"], default="online", help="Data provider mode")
    parser.add_argument("--fixture-dir", default=None, help="Fixture directory for offline mode")
    parser.add_argument("--fixture-profile", default="default", help="Fixture profile name (offline)")
    parser.add_argument("--fill-model", choices=["strict", "probabilistic"], default="strict", help="Fill simulation model")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for probabilistic fill model")
    parser.add_argument("--fill-alpha", type=float, default=1.5, help="Probabilistic model: exponential decay rate")
    parser.add_argument("--fill-pmax", type=float, default=0.20, help="Probabilistic model: max per-step fill probability")
    parser.add_argument("--fill-base-liquidity", type=float, default=0.10, help="Probabilistic model: base liquidity parameter")
    args = parser.parse_args()

    cfg = load_config()
    mm_cfg = cfg.get("market_making", {})

    run_id = f"mm-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
    bankroll = float(args.bankroll)
    quote_size_usd = float(args.quote_size_usd or mm_cfg.get("quote_size_usd", 10.0))
    k_ticks = int(args.k_ticks) if args.k_ticks is not None else int(mm_cfg.get("k_ticks", 2))
    tick_size = float(mm_cfg.get("tick_size", 0.01))
    max_spread_pct = mm_cfg.get("max_spread_pct", 0.05)
    max_spread_pct = float(max_spread_pct) if max_spread_pct is not None else None
    skew_ticks = float(mm_cfg.get("skew_ticks", 1.0))
    max_hold_time_sec = mm_cfg.get("max_hold_time_sec", 14400)
    max_total_exposure = float(args.max_exposure_usd or mm_cfg.get("max_exposure_usd", 5000))
    max_per_market = float(args.max_per_market_exposure_usd or mm_cfg.get("max_per_market_exposure_usd", 500))
    fee_bps = float(mm_cfg.get("fee_bps", 2.0))

    whitelist_path = pathlib.Path(args.whitelist) if args.whitelist else pathlib.Path("config/market_making_whitelist.json")
    whitelist = _load_whitelist(whitelist_path)

    db = Database(args.db)
    db.initialize_portfolio(bankroll)

    diagnostics = ExecutionDiagnostics(db_path=args.db, csv_path="market_making_fills.csv", live_mode=True)
    if args.data_mode == "offline":
        if not args.fixture_dir:
            raise SystemExit("--fixture-dir is required in offline mode")
        provider = FixtureProvider(pathlib.Path(args.fixture_dir), profile=args.fixture_profile)
    else:
        provider = PolymarketAPIProvider()
    rewards = RewardLedger(db_path=args.db)

    if args.fill_model == "probabilistic":
        fill_model = ProbabilisticFillModel(
            tick_size=tick_size,
            alpha=args.fill_alpha,
            base_liquidity=args.fill_base_liquidity,
            p_max=args.fill_pmax,
            seed=args.seed,
        )
    else:
        fill_model = DeterministicCrossingFillModel()

    markets = _select_markets(provider, whitelist=whitelist, max_markets=args.markets)
    if not markets:
        raise SystemExit("No markets selected (whitelist empty and no Tier-A markets found)")

    run_meta = {
        "run_id": run_id,
        "run_tag": args.run_tag,
        "bankroll": bankroll,
        "quote_size_usd": quote_size_usd,
        "k_ticks": k_ticks,
        "tick_size": tick_size,
        "max_spread_pct": max_spread_pct,
        "max_total_exposure": max_total_exposure,
        "max_per_market_exposure": max_per_market,
        "max_hold_time_sec": max_hold_time_sec,
        "fill_model": args.fill_model,
        "markets": markets,
    }
    rewards.add_reward(
        run_id=run_id,
        run_tag=args.run_tag,
        market_id=None,
        reward_usd=0.0,
        reward_source="unknown",
    )

    run_trust_gates(
        stage="pre_run",
        db=db,
        diagnostics=diagnostics,
        run_id=run_id,
        run_tag=args.run_tag,
        eps=1e-6,
        output_dir="reports",
        run_meta=run_meta,
    )

    engine = MarketMakingEngine(
        db=db,
        diagnostics=diagnostics,
        data_provider=provider,
        run_id=run_id,
        run_tag=args.run_tag,
        bankroll=bankroll,
        quote_size_usd=quote_size_usd,
        tick_size=tick_size,
        k_ticks=k_ticks,
        max_spread_pct=max_spread_pct,
        max_total_exposure_usd=max_total_exposure,
        max_per_market_exposure_usd=max_per_market,
        max_hold_time_sec=max_hold_time_sec,
        skew_ticks=skew_ticks,
        fee_bps=fee_bps,
        fill_model=fill_model,
    )

    start = time.time()
    fills = 0
    while True:
        for market_id in markets:
            snapshot, decision = engine.refresh_market(market_id, outcome="YES")
            engine.place_quotes(snapshot, decision)
            fills += engine.poll_fills(market_id, snapshot)
            if args.max_fills and fills >= args.max_fills:
                break
        if args.max_fills and fills >= args.max_fills:
            break
        if time.time() - start > args.max_runtime_min * 60:
            break
        reconciliation = db.reconcile_portfolio(starting_equity=bankroll)
        cash_balance = db.get_cash_balance()
        pnl_24h = db.calculate_24h_pnl()
        db.update_portfolio(reconciliation.get("total_value", cash_balance), cash_balance, pnl_24h)
        time.sleep(2)

    if args.data_mode == "offline":
        # Drain existing orders using fixture snapshots without placing new quotes.
        for _ in range(4):
            for market_id in markets:
                snapshot = provider.get_snapshot(market_id, outcome="YES", advance=True)
                fills += engine.poll_fills(market_id, snapshot)

    reconciliation = db.reconcile_portfolio(starting_equity=bankroll)
    cash_balance = db.get_cash_balance()
    pnl_24h = db.calculate_24h_pnl()
    db.update_portfolio(reconciliation.get("total_value", cash_balance), cash_balance, pnl_24h)

    run_trust_gates(
        stage="post_run",
        db=db,
        diagnostics=diagnostics,
        run_id=run_id,
        run_tag=args.run_tag,
        eps=1e-6,
        output_dir="reports",
        run_meta=run_meta,
    )

    print(f"Run complete: run_id={run_id} run_tag={args.run_tag} fills={fills}")


if __name__ == "__main__":
    main()
