"""Data provider abstraction for online/offline market making."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

from api_client import GammaAPIClient
from market_making_strategy import MarketSnapshot, compute_mid, compute_spread_pct


class DataProvider(Protocol):
    def get_markets(self) -> List[Dict[str, Any]]:
        ...

    def get_snapshot(self, market_id: str, outcome: str = "YES", advance: bool = True) -> MarketSnapshot:
        ...


class PolymarketAPIProvider:
    """Online data provider using Gamma + CLOB APIs.

    The CLOB order book often has extreme resting orders (0.01 bid / 0.99 ask)
    far from the actual market price.  When the book-derived spread exceeds a
    threshold we fall back to ``last_trade_price`` as the mid and derive
    synthetic bid/ask around it so the quoting engine gets usable numbers.
    """

    _SPREAD_SANITY_THRESHOLD = 0.50  # 50 % — anything wider is stale book

    def __init__(self, client: Optional[GammaAPIClient] = None):
        self._client = client or GammaAPIClient()
        self._market_cache: Dict[str, Dict[str, Any]] = {}

    def get_markets(self) -> List[Dict[str, Any]]:
        result = self._client.get_markets(active=True, closed=False)
        markets = result if isinstance(result, list) else result.get("markets", [])
        for m in markets:
            cid = m.get("conditionId")
            if cid:
                self._market_cache[cid] = m
        return markets

    def get_snapshot(self, market_id: str, outcome: str = "YES", advance: bool = True) -> MarketSnapshot:
        snap = self._client.get_market_snapshot_clob(market_id, outcome=outcome) or {}
        best_bid = snap.get("best_bid")
        best_ask = snap.get("best_ask")
        mid = snap.get("mid_price")
        last_trade_price = snap.get("last_trade_price")
        depth_bid_1 = snap.get("depth_bid_1")
        depth_ask_1 = snap.get("depth_ask_1")

        # Determine a reliable mid: prefer Gamma outcomePrices, then LTP.
        gamma_mid = self._gamma_mid(market_id)
        ref_mid = gamma_mid or last_trade_price

        # Detect stale/extreme book and synthesize tighter bid/ask.
        if ref_mid is not None and best_bid is not None and best_ask is not None:
            raw_spread = (best_ask - best_bid) / ref_mid if ref_mid > 0 else 999.0
            if raw_spread > self._SPREAD_SANITY_THRESHOLD:
                best_bid = ref_mid
                best_ask = ref_mid
                mid = ref_mid
                depth_bid_1 = None
                depth_ask_1 = None
        elif ref_mid is not None and (best_bid is None or best_ask is None):
            best_bid = ref_mid
            best_ask = ref_mid
            mid = ref_mid

        if mid is None:
            mid = compute_mid(best_bid, best_ask, mid)

        spread_pct = compute_spread_pct(best_bid, best_ask, mid)
        return MarketSnapshot(
            market_id=market_id,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid,
            spread_pct=spread_pct,
            depth_bid_1=depth_bid_1,
            depth_ask_1=depth_ask_1,
            last_trade_price=last_trade_price,
        )

    def _gamma_mid(self, market_id: str) -> Optional[float]:
        """Extract YES outcome price from cached Gamma market data."""
        m = self._market_cache.get(market_id)
        if not m:
            return None
        op_raw = m.get("outcomePrices")
        if isinstance(op_raw, str):
            try:
                op = json.loads(op_raw)
            except (json.JSONDecodeError, ValueError):
                return None
        else:
            op = op_raw
        if isinstance(op, list) and len(op) >= 1:
            try:
                return float(op[0])
            except (TypeError, ValueError):
                return None
        return None


class FixtureProvider:
    def __init__(self, fixture_dir: Path, profile: str = "default"):
        base = fixture_dir
        if profile != "default":
            base = fixture_dir / profile
        self._base = base
        self._markets = self._load_markets()
        self._snapshots: Dict[str, List[Dict[str, Any]]] = {}
        self._snapshot_idx: Dict[str, int] = {}

    def _load_markets(self) -> List[Dict[str, Any]]:
        path = self._base / "markets.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("markets.json must contain an array")
        return data

    def get_markets(self) -> List[Dict[str, Any]]:
        return list(self._markets)

    def _load_snapshots(self, market_id: str) -> List[Dict[str, Any]]:
        if market_id in self._snapshots:
            return self._snapshots[market_id]
        path = self._base / "snapshots" / f"{market_id}.jsonl"
        if not path.exists():
            raise FileNotFoundError(f"Missing snapshot fixture: {path}")
        rows: List[Dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rows.append(json.loads(line))
        if not rows:
            raise ValueError(f"Empty snapshot fixture: {path}")
        self._snapshots[market_id] = rows
        self._snapshot_idx[market_id] = 0
        return rows

    def reset_market(self, market_id: str) -> None:
        """Reset fixture index for a market back to the beginning."""
        if market_id in self._snapshot_idx:
            self._snapshot_idx[market_id] = 0

    def snapshot_count(self, market_id: str) -> int:
        """Return number of snapshots available for a market."""
        return len(self._load_snapshots(market_id))

    def get_snapshot(self, market_id: str, outcome: str = "YES", advance: bool = True) -> MarketSnapshot:
        rows = self._load_snapshots(market_id)
        idx = self._snapshot_idx.get(market_id, 0)
        if idx >= len(rows):
            idx = 0
        row = rows[idx]
        if advance:
            self._snapshot_idx[market_id] = idx + 1
        best_bid = row.get("best_bid")
        best_ask = row.get("best_ask")
        mid = row.get("mid_price")
        last_trade_price = row.get("last_trade_price")
        spread_pct = compute_spread_pct(best_bid, best_ask, compute_mid(best_bid, best_ask, mid))
        return MarketSnapshot(
            market_id=market_id,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid,
            spread_pct=spread_pct,
            depth_bid_1=row.get("depth_bid_1"),
            depth_ask_1=row.get("depth_ask_1"),
            last_trade_price=last_trade_price,
        )
