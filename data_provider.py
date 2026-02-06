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
    def __init__(self, client: Optional[GammaAPIClient] = None):
        self._client = client or GammaAPIClient()

    def get_markets(self) -> List[Dict[str, Any]]:
        return self._client.get_markets(active=True, closed=False).get("markets", [])

    def get_snapshot(self, market_id: str, outcome: str = "YES", advance: bool = True) -> MarketSnapshot:
        snap = self._client.get_market_snapshot_clob(market_id, outcome=outcome) or {}
        best_bid = snap.get("best_bid")
        best_ask = snap.get("best_ask")
        mid = snap.get("mid_price")
        last_trade_price = snap.get("last_trade_price")
        spread_pct = compute_spread_pct(best_bid, best_ask, compute_mid(best_bid, best_ask, mid))
        return MarketSnapshot(
            market_id=market_id,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=mid,
            spread_pct=spread_pct,
            depth_bid_1=snap.get("depth_bid_1"),
            depth_ask_1=snap.get("depth_ask_1"),
            last_trade_price=last_trade_price,
        )


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
