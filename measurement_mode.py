"""Helpers for controlled slippage measurement experiments."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class MarketCandidate:
    market_id: str
    market_slug: str
    outcome: str
    spread_pct: Optional[float]
    depth_sum: Optional[float]
    tier: str


def build_synthetic_baseline(decision_ts: datetime, reference_price: float) -> Dict[str, Any]:
    """Build synthetic whale baseline fields for measurement mode."""
    return {
        "whale_signal_ts": decision_ts,
        "whale_entry_ref_price": reference_price,
        "whale_ref_type": "synthetic",
    }


class MeasurementSelector:
    """Deterministic selector that buckets markets into liquidity tiers."""

    @staticmethod
    def tier_for_snapshot(spread_pct: Optional[float], depth_sum: Optional[float]) -> str:
        spread = spread_pct if spread_pct is not None else 1.0
        depth = depth_sum if depth_sum is not None else 0.0
        if spread <= 0.01 and depth >= 1000:
            return "A"
        if spread <= 0.03 and depth >= 250:
            return "B"
        return "C"

    def build_candidates(
        self,
        target_positions: List[Any],
        snapshots: Dict[str, Dict[str, Optional[float]]],
    ) -> List[MarketCandidate]:
        candidates: List[MarketCandidate] = []
        for pos in target_positions:
            market_id = getattr(pos, "market", "")
            if not market_id:
                continue
            snap = snapshots.get(market_id, {})
            best_bid = snap.get("best_bid")
            best_ask = snap.get("best_ask")
            mid = snap.get("mid_price")
            spread_pct: Optional[float] = None
            if best_bid is not None and best_ask is not None and mid not in (None, 0):
                spread_pct = (best_ask - best_bid) / mid
            depth_bid = snap.get("depth_bid_1")
            depth_ask = snap.get("depth_ask_1")
            depth_sum = None
            if depth_bid is not None or depth_ask is not None:
                depth_sum = float(depth_bid or 0.0) + float(depth_ask or 0.0)
            tier = self.tier_for_snapshot(spread_pct, depth_sum)
            candidates.append(
                MarketCandidate(
                    market_id=market_id,
                    market_slug=getattr(pos, "market_slug", "") or "",
                    outcome=(getattr(pos, "outcome", "YES") or "YES"),
                    spread_pct=spread_pct,
                    depth_sum=depth_sum,
                    tier=tier,
                )
            )

        # Deterministic ordering: tier first, then market_id
        candidates.sort(key=lambda c: (c.tier, c.market_id))
        return candidates

    def select_cycle(
        self,
        candidates: List[MarketCandidate],
        n: int,
        market_filter: Optional[str] = None,
    ) -> List[MarketCandidate]:
        if market_filter and market_filter.upper() in {"A", "B", "C"}:
            filtered = [c for c in candidates if c.tier == market_filter.upper()]
        else:
            filtered = list(candidates)

        if not filtered:
            return []

        tiers: Dict[str, List[MarketCandidate]] = {"A": [], "B": [], "C": []}
        for candidate in filtered:
            tiers[candidate.tier].append(candidate)

        order = ["A", "B", "C"] if not market_filter else [market_filter.upper()]
        idx = {"A": 0, "B": 0, "C": 0}
        selected: List[MarketCandidate] = []
        while len(selected) < n:
            progressed = False
            for tier in order:
                bucket = tiers.get(tier, [])
                if not bucket:
                    continue
                selected.append(bucket[idx[tier] % len(bucket)])
                idx[tier] += 1
                progressed = True
                if len(selected) >= n:
                    break
            if not progressed:
                break
        return selected
