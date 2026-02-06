"""Minimal market making engine with passive limit quotes."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Dict, Optional, Tuple

from data_provider import DataProvider
from database import Database
from execution_diagnostics import ExecutionDiagnostics
from fill_model import DeterministicCrossingFillModel, FillModel
from market_making_strategy import (
    MarketSnapshot,
    InventoryState,
    QuoteDecision,
    compute_mid,
    compute_spread_pct,
    decide_quotes,
    validate_snapshot,
)
from measurement_mode import MeasurementSelector
from pnl import compute_shares


@dataclass
class ActiveOrder:
    order_id: str
    market_id: str
    side: str  # "buy" or "sell"
    price: float
    qty_shares: float
    created_at: datetime
    snapshot: MarketSnapshot


@dataclass
class FillEvent:
    order: ActiveOrder
    fill_price: float
    fill_ts: datetime


class MarketMakingEngine:
    def __init__(
        self,
        *,
        db: Database,
        diagnostics: ExecutionDiagnostics,
        data_provider: DataProvider,
        run_id: str,
        run_tag: str,
        bankroll: float,
        quote_size_usd: float,
        tick_size: float,
        k_ticks: int,
        max_spread_pct: Optional[float],
        max_total_exposure_usd: float,
        max_per_market_exposure_usd: float,
        max_hold_time_sec: Optional[int],
        skew_ticks: float,
        fee_bps: float,
        fill_model: Optional[FillModel] = None,
    ):
        self.db = db
        self.diagnostics = diagnostics
        self.data_provider = data_provider
        self.run_id = run_id
        self.run_tag = run_tag
        self.bankroll = bankroll
        self.quote_size_usd = quote_size_usd
        self.tick_size = tick_size
        self.k_ticks = k_ticks
        self.max_spread_pct = max_spread_pct
        self.max_total_exposure_usd = max_total_exposure_usd
        self.max_per_market_exposure_usd = max_per_market_exposure_usd
        self.max_hold_time_sec = max_hold_time_sec
        self.skew_ticks = skew_ticks
        self.fee_bps = fee_bps
        self.fill_model: FillModel = fill_model or DeterministicCrossingFillModel()
        self._orders: Dict[Tuple[str, str], ActiveOrder] = {}
        self._order_seq = 0
        self._tier_selector = MeasurementSelector()

    def _next_order_id(self) -> str:
        self._order_seq += 1
        return f"{self.run_id}-{self._order_seq}"

    def _inventory_state(self, market_id: str) -> InventoryState:
        open_positions = self.db.get_open_positions()
        net = 0.0
        gross = 0.0
        oldest_hold = None
        now = datetime.now(UTC)
        for pos in open_positions:
            if pos.get("market") != market_id:
                continue
            size = float(pos.get("size") or 0.0)
            side = (pos.get("side") or "BUY").upper()
            if side == "BUY":
                net += size
                gross += abs(size)
            else:
                net -= size
                gross += abs(size)
            ts = pos.get("timestamp")
            if ts:
                try:
                    opened = datetime.fromisoformat(ts)
                    if opened.tzinfo is None:
                        opened = opened.replace(tzinfo=UTC)
                    age = (now - opened).total_seconds()
                    if oldest_hold is None or age > oldest_hold:
                        oldest_hold = age
                except ValueError:
                    continue
        return InventoryState(net_usd=net, gross_usd=gross, oldest_hold_sec=oldest_hold)

    def _total_gross_exposure(self) -> float:
        gross = 0.0
        for pos in self.db.get_open_positions():
            gross += abs(float(pos.get("size") or 0.0))
        return gross

    def _snapshot_for_market(self, market_id: str, outcome: str = "YES") -> MarketSnapshot:
        return self.data_provider.get_snapshot(market_id, outcome=outcome)

    def _tier_for_snapshot(self, snapshot: MarketSnapshot) -> str:
        return self._tier_selector.tier_for_snapshot(snapshot.spread_pct, _depth_sum(snapshot))

    def build_quote(self, snapshot: MarketSnapshot) -> QuoteDecision:
        inventory = self._inventory_state(snapshot.market_id)
        inventory = InventoryState(
            net_usd=inventory.net_usd,
            gross_usd=self._total_gross_exposure(),
            oldest_hold_sec=inventory.oldest_hold_sec,
        )
        decision = decide_quotes(
            snapshot,
            inventory,
            tick_size=self.tick_size,
            k_ticks=self.k_ticks,
            max_spread_pct=self.max_spread_pct,
            max_per_market_exposure_usd=self.max_per_market_exposure_usd,
            max_total_exposure_usd=self.max_total_exposure_usd,
            skew_ticks=self.skew_ticks,
        )
        if self.max_hold_time_sec and inventory.oldest_hold_sec is not None:
            if inventory.oldest_hold_sec > self.max_hold_time_sec:
                # Only allow reducing exposure when inventory is stale.
                if inventory.net_usd > 0:
                    decision = QuoteDecision(
                        decision.bid_price,
                        decision.ask_price,
                        False,
                        decision.place_ask,
                        decision.pause_reason,
                    )
                else:
                    decision = QuoteDecision(
                        decision.bid_price,
                        decision.ask_price,
                        decision.place_bid,
                        False,
                        decision.pause_reason,
                    )
        if inventory.net_usd <= 0:
            decision = QuoteDecision(
                decision.bid_price,
                decision.ask_price,
                decision.place_bid,
                False,
                decision.pause_reason,
            )
        return decision

    def refresh_market(self, market_id: str, outcome: str = "YES") -> Tuple[MarketSnapshot, QuoteDecision]:
        snapshot = self._snapshot_for_market(market_id, outcome=outcome)
        reason = validate_snapshot(snapshot)
        if reason:
            return snapshot, QuoteDecision(None, None, False, False, reason)
        return snapshot, self.build_quote(snapshot)

    def place_quotes(self, snapshot: MarketSnapshot, decision: QuoteDecision) -> None:
        if decision.pause_reason:
            self.cancel_market(snapshot.market_id)
            return

        if not decision.place_bid and not decision.place_ask:
            self.cancel_market(snapshot.market_id)
            return

        if snapshot.best_ask is not None and decision.bid_price is not None:
            if decision.bid_price >= snapshot.best_ask:
                decision = QuoteDecision(
                    decision.bid_price,
                    decision.ask_price,
                    False,
                    decision.place_ask,
                    "bid_crosses_ask",
                )
        if snapshot.best_bid is not None and decision.ask_price is not None:
            if decision.ask_price <= snapshot.best_bid:
                decision = QuoteDecision(
                    decision.bid_price,
                    decision.ask_price,
                    decision.place_bid,
                    False,
                    "ask_crosses_bid",
                )

        now = datetime.now(UTC)
        if decision.place_bid and decision.bid_price is not None:
            self._upsert_order(snapshot, "buy", decision.bid_price, now)
        else:
            self._cancel_order(snapshot.market_id, "buy")

        if decision.place_ask and decision.ask_price is not None:
            self._upsert_order(snapshot, "sell", decision.ask_price, now)
        else:
            self._cancel_order(snapshot.market_id, "sell")

    def _upsert_order(self, snapshot: MarketSnapshot, side: str, price: float, now: datetime) -> None:
        key = (snapshot.market_id, side)
        existing = self._orders.get(key)
        if existing and abs(existing.price - price) < 1e-9:
            return

        order_id = self._next_order_id()
        qty_shares = compute_shares(self.quote_size_usd, price)
        order = ActiveOrder(
            order_id=order_id,
            market_id=snapshot.market_id,
            side=side,
            price=price,
            qty_shares=qty_shares,
            created_at=now,
            snapshot=snapshot,
        )
        self._orders[key] = order
        self._record_order_sent(order)

    def _cancel_order(self, market_id: str, side: str) -> None:
        key = (market_id, side)
        if key in self._orders:
            del self._orders[key]

    def cancel_market(self, market_id: str) -> None:
        self._cancel_order(market_id, "buy")
        self._cancel_order(market_id, "sell")

    def _record_order_sent(self, order: ActiveOrder) -> None:
        payload = {
            "run_id": self.run_id,
            "run_tag": self.run_tag,
            "order_id": order.order_id,
            "trade_id": None,
            "market_id": order.market_id,
            "market_slug": order.snapshot.market_id,
            "side": order.side,
            "order_type": "limit",
            "qty_shares": order.qty_shares,
            "intended_limit_price": order.price,
            "time_in_force": "GTC",
            "whale_signal_ts": None,
            "whale_entry_ref_price": None,
            "whale_ref_type": "unknown",
            "our_decision_ts": order.created_at,
            "order_sent_ts": order.created_at,
            "exchange_ack_ts": order.created_at,
            "fill_ts": None,
            "best_bid": order.snapshot.best_bid,
            "best_ask": order.snapshot.best_ask,
            "mid_price": compute_mid(order.snapshot.best_bid, order.snapshot.best_ask, order.snapshot.mid_price),
            "depth_bid_1": order.snapshot.depth_bid_1,
            "depth_ask_1": order.snapshot.depth_ask_1,
            "depth_bid_2": None,
            "depth_ask_2": None,
            "last_trade_price": order.snapshot.last_trade_price,
            "fill_price": None,
            "entry_price_source": "quote",
            "current_price_source": "quote",
            "exit_price_source": "unknown",
            "fill_price_source": "unknown",
            "filled_shares": None,
            "fees_usd": 0.0,
            "is_partial_fill": False,
            "fill_count": 0,
            "liquidity_tier": self._tier_for_snapshot(order.snapshot),
        }
        self.diagnostics.record_order_sent(payload)

    def poll_fills(self, market_id: str, snapshot: MarketSnapshot) -> int:
        fills = 0
        now = datetime.now(UTC)
        for side in ("buy", "sell"):
            key = (market_id, side)
            order = self._orders.get(key)
            if not order:
                continue
            decision = self.fill_model.should_fill(order.side, order.price, snapshot, now)
            if not decision.fill or decision.fill_price is None:
                continue
            fill = FillEvent(order=order, fill_price=decision.fill_price, fill_ts=now)
            self._handle_fill(fill)
            del self._orders[key]
            fills += 1
        return fills

    def _handle_fill(self, fill: FillEvent) -> None:
        order = fill.order
        size_usd = order.qty_shares * fill.fill_price
        fees_usd = abs(size_usd) * (self.fee_bps / 10000.0)
        trade_id = self._apply_trade(order.market_id, order.side, size_usd, fill.fill_price)

        payload = {
            "run_id": self.run_id,
            "run_tag": self.run_tag,
            "order_id": order.order_id,
            "trade_id": trade_id,
            "market_id": order.market_id,
            "market_slug": order.snapshot.market_id,
            "side": order.side,
            "order_type": "limit",
            "qty_shares": order.qty_shares,
            "intended_limit_price": order.price,
            "time_in_force": "GTC",
            "whale_signal_ts": None,
            "whale_entry_ref_price": None,
            "whale_ref_type": "unknown",
            "our_decision_ts": order.created_at,
            "order_sent_ts": order.created_at,
            "exchange_ack_ts": order.created_at,
            "fill_ts": fill.fill_ts,
            "best_bid": order.snapshot.best_bid,
            "best_ask": order.snapshot.best_ask,
            "mid_price": compute_mid(order.snapshot.best_bid, order.snapshot.best_ask, order.snapshot.mid_price),
            "depth_bid_1": order.snapshot.depth_bid_1,
            "depth_ask_1": order.snapshot.depth_ask_1,
            "depth_bid_2": None,
            "depth_ask_2": None,
            "last_trade_price": order.snapshot.last_trade_price,
            "fill_price": fill.fill_price,
            "entry_price_source": "fill",
            "current_price_source": "fill",
            "exit_price_source": "unknown",
            "fill_price_source": "fill",
            "filled_shares": order.qty_shares,
            "fees_usd": fees_usd,
            "is_partial_fill": False,
            "fill_count": 1,
            "liquidity_tier": self._tier_for_snapshot(order.snapshot),
        }
        self.diagnostics.record_fill(payload)

    def _apply_trade(self, market_id: str, side: str, size_usd: float, price: float) -> Optional[int]:
        # Market making inventory is long-only. Only close existing longs on sell.
        if side == "buy":
            return self.db.add_trade(
                market=market_id,
                side="BUY",
                size=size_usd,
                price=price,
                target_wallet="market_making",
                market_slug=market_id,
                outcome="YES",
                entry_price_source="fill",
                current_price_source="fill",
                run_id=self.run_id,
                run_tag=self.run_tag,
            )

        open_pos = _find_oldest_open(self.db, market_id, side="BUY")
        if not open_pos:
            # Skip opening shorts in this experiment.
            return None
        trade_id = int(open_pos["id"])
        self.db.close_trade(
            trade_id,
            exit_price=price,
            close_size=float(open_pos.get("size") or size_usd),
            exit_price_source="fill",
            fill_price_source="fill",
        )
        return trade_id


def _find_oldest_open(db: Database, market_id: str, side: str = "BUY") -> Optional[Dict[str, object]]:
    for pos in reversed(db.get_open_positions()):
        if pos.get("market") != market_id:
            continue
        if (pos.get("side") or "").upper() != side.upper():
            continue
        return pos
    return None


def _depth_sum(snapshot: MarketSnapshot) -> Optional[float]:
    if snapshot.depth_bid_1 is None and snapshot.depth_ask_1 is None:
        return None
    return float(snapshot.depth_bid_1 or 0.0) + float(snapshot.depth_ask_1 or 0.0)
