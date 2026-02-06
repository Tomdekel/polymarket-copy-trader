"""Market making quote generation and inventory controls."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

PRICE_EPS = 1e-9


@dataclass(frozen=True)
class MarketSnapshot:
    market_id: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    mid_price: Optional[float]
    spread_pct: Optional[float]
    depth_bid_1: Optional[float]
    depth_ask_1: Optional[float]
    last_trade_price: Optional[float]


@dataclass(frozen=True)
class InventoryState:
    net_usd: float
    gross_usd: float
    oldest_hold_sec: Optional[float]


@dataclass(frozen=True)
class QuoteDecision:
    bid_price: Optional[float]
    ask_price: Optional[float]
    place_bid: bool
    place_ask: bool
    pause_reason: Optional[str]


def compute_mid(best_bid: Optional[float], best_ask: Optional[float], mid_price: Optional[float]) -> Optional[float]:
    if best_bid is not None and best_ask is not None:
        return (best_bid + best_ask) / 2.0
    return mid_price


def validate_snapshot(snapshot: MarketSnapshot, eps: float = PRICE_EPS) -> Optional[str]:
    if snapshot.best_bid is None or snapshot.best_ask is None:
        return "missing_bid_ask"
    if snapshot.best_bid < 0 or snapshot.best_ask < 0:
        return "negative_bid_ask"
    if snapshot.best_bid > snapshot.best_ask + eps:
        return "bid_gt_ask"
    mid = compute_mid(snapshot.best_bid, snapshot.best_ask, snapshot.mid_price)
    if mid is None:
        return "missing_mid"
    if snapshot.mid_price is not None and abs(snapshot.mid_price - mid) > eps:
        return "mid_mismatch"
    if mid <= 0 or mid >= 1:
        return "mid_out_of_range"
    return None


def compute_spread_pct(best_bid: Optional[float], best_ask: Optional[float], mid: Optional[float]) -> Optional[float]:
    if best_bid is None or best_ask is None or mid in (None, 0):
        return None
    return (best_ask - best_bid) / mid


def quote_prices(
    mid: float,
    tick_size: float,
    k_ticks: int,
    skew_ticks: float = 0.0,
    skew_direction: float = 0.0,
) -> Tuple[float, float]:
    # skew_direction: +1 means skew away from buys (long), -1 means skew away from sells (short).
    # k_ticks=0 => minimal spread around mid using half-tick.
    base_ticks = float(k_ticks)
    base = (tick_size / 2.0) if base_ticks == 0 else base_ticks * float(tick_size)
    skew = float(skew_ticks) * float(tick_size) * float(skew_direction)
    bid = mid - base - skew
    ask = mid + base - skew
    return bid, ask


def decide_quotes(
    snapshot: MarketSnapshot,
    inventory: InventoryState,
    *,
    tick_size: float,
    k_ticks: int,
    max_spread_pct: Optional[float],
    max_per_market_exposure_usd: float,
    max_total_exposure_usd: float,
    skew_ticks: float = 0.0,
) -> QuoteDecision:
    mid = compute_mid(snapshot.best_bid, snapshot.best_ask, snapshot.mid_price)
    if mid is None:
        return QuoteDecision(None, None, False, False, "missing_mid")

    spread_pct = compute_spread_pct(snapshot.best_bid, snapshot.best_ask, mid)
    if max_spread_pct is not None and spread_pct is not None and spread_pct > max_spread_pct:
        return QuoteDecision(None, None, False, False, "spread_too_wide")

    net = inventory.net_usd
    gross = inventory.gross_usd

    if max_per_market_exposure_usd <= 0 or max_total_exposure_usd <= 0:
        return QuoteDecision(None, None, False, False, "invalid_exposure_caps")

    skew_direction = 0.0
    if max_per_market_exposure_usd > 0:
        skew_direction = 1.0 if net > 0 else (-1.0 if net < 0 else 0.0)
    bid, ask = quote_prices(mid, tick_size, k_ticks, skew_ticks=skew_ticks, skew_direction=skew_direction)

    # Clamp to [0,1] and ensure monotonic ordering.
    if bid <= 0 or ask >= 1 or bid >= ask:
        return QuoteDecision(None, None, False, False, "invalid_quote_bounds")

    place_bid = True
    place_ask = True

    # Exposure caps: if max reached, only allow quotes that reduce exposure.
    if abs(net) >= max_per_market_exposure_usd - PRICE_EPS:
        if net > 0:
            place_bid = False
        elif net < 0:
            place_ask = False

    if gross >= max_total_exposure_usd - PRICE_EPS:
        if net > 0:
            place_bid = False
        elif net < 0:
            place_ask = False
        else:
            place_bid = False
            place_ask = False

    return QuoteDecision(bid, ask, place_bid, place_ask, None)
