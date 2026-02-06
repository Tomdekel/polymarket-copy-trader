"""Pluggable fill-simulation models for the market-making engine.

Two models are provided:

* **DeterministicCrossingFillModel** — the original exact-crossing logic.
  A buy fills iff ``last_trade_price <= quote_price``; a sell fills iff
  ``last_trade_price >= quote_price``.

* **ProbabilisticFillModel** — conservative probabilistic model.
  ``p = min(p_max, base_liquidity * exp(-alpha * dist_ticks))``
  where ``dist_ticks = abs(quote_price - ref_price) / tick_size``.
  Deterministic for a given seed.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Protocol

from market_making_strategy import MarketSnapshot


@dataclass(frozen=True)
class FillDecision:
    """Result of a fill-model evaluation."""

    fill: bool
    fill_price: Optional[float]
    reason: str
    diagnostics: Dict[str, Any] = field(default_factory=dict)


class FillModel(Protocol):
    """Interface that every fill model must satisfy."""

    def should_fill(
        self,
        order_side: str,
        order_price: float,
        snapshot: MarketSnapshot,
        t: datetime,
    ) -> FillDecision:
        """Decide whether a resting order would be filled given *snapshot*.

        Parameters
        ----------
        order_side:
            ``"buy"`` or ``"sell"``.
        order_price:
            Limit price of the resting order.
        snapshot:
            Current market snapshot (bid/ask/mid/last_trade_price).
        t:
            Current evaluation timestamp.

        Returns
        -------
        FillDecision
        """
        ...


# ---------------------------------------------------------------------------
# Model A — deterministic crossing (original behaviour)
# ---------------------------------------------------------------------------

class DeterministicCrossingFillModel:
    """Fill iff last_trade_price crosses the order price.

    This is the logic previously hard-coded in ``_order_should_fill``.
    """

    def should_fill(
        self,
        order_side: str,
        order_price: float,
        snapshot: MarketSnapshot,
        t: datetime,
    ) -> FillDecision:
        ltp = snapshot.last_trade_price
        if ltp is None:
            return FillDecision(
                fill=False,
                fill_price=None,
                reason="no_last_trade_price",
                diagnostics={"model": "deterministic"},
            )

        if order_side == "buy":
            crossed = ltp <= order_price + 1e-9
        else:
            crossed = ltp >= order_price - 1e-9

        if crossed:
            return FillDecision(
                fill=True,
                fill_price=ltp,
                reason="price_crossed",
                diagnostics={"model": "deterministic", "ltp": ltp},
            )

        return FillDecision(
            fill=False,
            fill_price=None,
            reason="no_crossing",
            diagnostics={"model": "deterministic", "ltp": ltp, "order_price": order_price},
        )


# ---------------------------------------------------------------------------
# Model B — probabilistic fill (MVP)
# ---------------------------------------------------------------------------

class ProbabilisticFillModel:
    """Conservative probabilistic fill model.

    ``p = min(p_max, base_liquidity * exp(-alpha * dist_ticks))``

    * ``dist_ticks`` = ``abs(quote_price - ref_price) / tick_size``
    * ``ref_price``  = ``last_trade_price`` (preferred) or ``mid_price``
    * ``base_liquidity`` — scales the curve vertically (higher = more fills)
    * ``alpha`` — controls how quickly fill probability decays with distance
    * ``p_max`` — hard ceiling on per-evaluation fill probability

    Deterministic for a given *seed*.
    """

    def __init__(
        self,
        *,
        tick_size: float = 0.01,
        alpha: float = 1.5,
        base_liquidity: float = 0.10,
        p_max: float = 0.20,
        seed: int = 42,
    ):
        self.tick_size = tick_size
        self.alpha = alpha
        self.base_liquidity = base_liquidity
        self.p_max = p_max
        self._rng = random.Random(seed)

    def should_fill(
        self,
        order_side: str,
        order_price: float,
        snapshot: MarketSnapshot,
        t: datetime,
    ) -> FillDecision:
        ref_price = snapshot.last_trade_price or snapshot.mid_price
        if ref_price is None:
            return FillDecision(
                fill=False,
                fill_price=None,
                reason="no_ref_price",
                diagnostics={"model": "probabilistic"},
            )

        dist = abs(order_price - ref_price)
        dist_ticks = dist / self.tick_size if self.tick_size > 0 else 0.0
        p_raw = self.base_liquidity * math.exp(-self.alpha * dist_ticks)
        p = min(self.p_max, p_raw)

        roll = self._rng.random()
        filled = roll < p

        diag: Dict[str, Any] = {
            "model": "probabilistic",
            "ref_price": ref_price,
            "dist": round(dist, 6),
            "dist_ticks": round(dist_ticks, 4),
            "p_raw": round(p_raw, 6),
            "p_capped": round(p, 6),
            "roll": round(roll, 6),
        }

        if not filled:
            return FillDecision(
                fill=False,
                fill_price=None,
                reason="prob_no_fill",
                diagnostics=diag,
            )

        # Fill price: midpoint between order price and ref_price.
        # This avoids filling exactly at our limit which overstates P&L.
        fill_price = (order_price + ref_price) / 2.0
        # Clamp to [0, 1] for prediction markets.
        fill_price = max(0.0, min(1.0, fill_price))

        return FillDecision(
            fill=True,
            fill_price=fill_price,
            reason="prob_fill",
            diagnostics=diag,
        )
