"""Slippage diagnostics pipeline (structured logging + export helpers)."""

from __future__ import annotations

import csv
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

logger = logging.getLogger(__name__)

PRICE_EPS = 1e-9

SLIPPAGE_EXPORT_COLUMNS = [
    "run_id",
    "run_tag",
    "order_id",
    "trade_id",
    "market_id",
    "market_slug",
    "side",
    "order_type",
    "qty_shares",
    "intended_limit_price",
    "time_in_force",
    "whale_signal_ts",
    "whale_entry_ref_price",
    "whale_ref_type",
    "our_decision_ts",
    "order_sent_ts",
    "exchange_ack_ts",
    "fill_ts",
    "best_bid",
    "best_ask",
    "mid_price",
    "spread_abs",
    "spread_pct",
    "depth_bid_1",
    "depth_ask_1",
    "depth_bid_2",
    "depth_ask_2",
    "last_trade_price",
    "fill_price",
    "entry_price_source",
    "current_price_source",
    "exit_price_source",
    "fill_price_source",
    "filled_shares",
    "fees_usd",
    "is_partial_fill",
    "fill_count",
    "latency_ms",
    "quote_slippage_pct",
    "half_spread_pct",
    "baseline_slippage_pct",
    "spread_crossed",
    "impact_proxy_pct",
    "liquidity_tier",
]


class OrderExecutionRecord(BaseModel):
    """Structured order/fill log with derived slippage metrics."""

    model_config = ConfigDict(extra="forbid")

    # Identifiers
    run_id: str
    run_tag: str = "default"
    order_id: str
    trade_id: Optional[int] = None
    market_id: str
    market_slug: Optional[str] = None

    # Order intent
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit"]
    qty_shares: float = Field(ge=0)
    intended_limit_price: Optional[float] = None
    time_in_force: Optional[str] = None

    # Whale reference
    whale_signal_ts: Optional[datetime] = None
    whale_entry_ref_price: Optional[float] = None
    whale_ref_type: Literal["snapshot", "avg_fill", "vwap", "synthetic", "unknown"] = "unknown"

    # Timestamps
    our_decision_ts: Optional[datetime] = None
    order_sent_ts: Optional[datetime] = None
    exchange_ack_ts: Optional[datetime] = None
    fill_ts: Optional[datetime] = None

    # Snapshot at order send
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    mid_price: Optional[float] = None
    spread_abs: Optional[float] = None
    spread_pct: Optional[float] = None
    depth_bid_1: Optional[float] = None
    depth_ask_1: Optional[float] = None
    depth_bid_2: Optional[float] = None
    depth_ask_2: Optional[float] = None
    last_trade_price: Optional[float] = None

    # Fill details
    fill_price: Optional[float] = None
    entry_price_source: Literal["fill", "quote", "mark", "whale_ref", "placeholder", "unknown"] = "unknown"
    current_price_source: Literal["fill", "quote", "mark", "whale_ref", "placeholder", "unknown"] = "unknown"
    exit_price_source: Literal["fill", "quote", "mark", "whale_ref", "placeholder", "unknown"] = "unknown"
    fill_price_source: Literal["fill", "quote", "mark", "whale_ref", "placeholder", "unknown"] = "unknown"
    filled_shares: Optional[float] = None
    fees_usd: float = 0.0
    is_partial_fill: bool = False
    fill_count: int = 0

    # Derived metrics
    latency_ms: Optional[float] = None
    quote_slippage_pct: Optional[float] = None
    half_spread_pct: Optional[float] = None
    baseline_slippage_pct: Optional[float] = None
    spread_crossed: Optional[bool] = None
    impact_proxy_pct: Optional[float] = None
    liquidity_tier: Optional[str] = None

    @model_validator(mode="after")
    def compute_derived_metrics(self) -> "OrderExecutionRecord":
        for field in (
            "intended_limit_price",
            "whale_entry_ref_price",
            "best_bid",
            "best_ask",
            "mid_price",
            "last_trade_price",
            "fill_price",
        ):
            value = getattr(self, field)
            if value is None:
                continue
            if value < 0 or value > 1:
                raise ValueError(f"{field} must be in [0,1], got {value}")

        if self.best_bid is not None and self.best_ask is not None and self.best_bid > self.best_ask + PRICE_EPS:
            raise ValueError(f"Invalid snapshot: best_bid ({self.best_bid}) > best_ask ({self.best_ask})")

        computed_mid: Optional[float] = None
        if self.best_bid is not None and self.best_ask is not None:
            computed_mid = (self.best_bid + self.best_ask) / 2.0
            if self.mid_price is None:
                self.mid_price = computed_mid
            elif abs(self.mid_price - computed_mid) > PRICE_EPS:
                raise ValueError(
                    f"mid_price mismatch: mid={self.mid_price}, expected={(self.best_bid + self.best_ask) / 2.0}"
                )

            self.spread_abs = self.best_ask - self.best_bid
            if self.spread_abs < -PRICE_EPS:
                raise ValueError(f"spread_abs must be >= 0, got {self.spread_abs}")

        if self.mid_price is not None and self.spread_abs is not None and self.mid_price > 0:
            self.spread_pct = self.spread_abs / self.mid_price

        if self.best_ask is not None and self.mid_price is not None and self.mid_price > 0:
            self.half_spread_pct = (self.best_ask - self.mid_price) / self.mid_price

        latency_base = self.fill_ts or self.order_sent_ts
        if latency_base is not None and self.whale_signal_ts is not None:
            self.latency_ms = (latency_base - self.whale_signal_ts).total_seconds() * 1000.0

        has_snapshot = self.mid_price not in (None, 0) and self.best_bid is not None and self.best_ask is not None
        if self.fill_price is not None and self.fill_price_source == "fill" and has_snapshot:
            self.quote_slippage_pct = (self.fill_price - self.mid_price) / self.mid_price

        if self.fill_price is not None and self.fill_price_source == "fill" and self.whale_entry_ref_price not in (None, 0):
            self.baseline_slippage_pct = (
                (self.fill_price - self.whale_entry_ref_price) / self.whale_entry_ref_price
            )

        if self.fill_price is not None and self.fill_price_source == "fill" and self.best_bid is not None and self.best_ask is not None:
            if self.side == "buy":
                self.spread_crossed = self.fill_price >= self.best_ask - PRICE_EPS
                if self.best_ask > 0:
                    self.impact_proxy_pct = (self.fill_price - self.best_ask) / self.best_ask
            else:
                self.spread_crossed = self.fill_price <= self.best_bid + PRICE_EPS
                if self.best_bid > 0:
                    self.impact_proxy_pct = (self.best_bid - self.fill_price) / self.best_bid

        return self


class ExecutionDiagnostics:
    """Durable structured slippage recorder backed by SQLite."""

    def __init__(
        self,
        db_path: str = "trades.db",
        csv_path: str = "slippage.csv",
        live_mode: bool = False,
    ):
        self.db_path = db_path
        self.csv_path = csv_path
        self.live_mode = live_mode
        self._lock = Lock()
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_table(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    run_tag TEXT NOT NULL DEFAULT 'default',
                    order_id TEXT NOT NULL,
                    trade_id INTEGER,
                    market_id TEXT NOT NULL,
                    market_slug TEXT,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    qty_shares REAL NOT NULL,
                    intended_limit_price REAL,
                    time_in_force TEXT,
                    whale_signal_ts TEXT,
                    whale_entry_ref_price REAL,
                    whale_ref_type TEXT NOT NULL,
                    our_decision_ts TEXT,
                    order_sent_ts TEXT,
                    exchange_ack_ts TEXT,
                    fill_ts TEXT,
                    best_bid REAL,
                    best_ask REAL,
                    mid_price REAL,
                    spread_abs REAL,
                    spread_pct REAL,
                    depth_bid_1 REAL,
                    depth_ask_1 REAL,
                    depth_bid_2 REAL,
                    depth_ask_2 REAL,
                    last_trade_price REAL,
                    fill_price REAL,
                    entry_price_source TEXT,
                    current_price_source TEXT,
                    exit_price_source TEXT,
                    fill_price_source TEXT,
                    filled_shares REAL,
                    fees_usd REAL,
                    is_partial_fill INTEGER,
                    fill_count INTEGER,
                    latency_ms REAL,
                    quote_slippage_pct REAL,
                    half_spread_pct REAL,
                    baseline_slippage_pct REAL,
                    spread_crossed INTEGER,
                    impact_proxy_pct REAL,
                    liquidity_tier TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(run_id, order_id)
                )
                """
            )
            existing_cols = {
                row[1] for row in conn.execute("PRAGMA table_info(execution_records)").fetchall()
            }
            for col, col_type in [
                ("run_tag", "TEXT DEFAULT 'default'"),
                ("entry_price_source", "TEXT"),
                ("current_price_source", "TEXT"),
                ("exit_price_source", "TEXT"),
                ("fill_price_source", "TEXT"),
                ("liquidity_tier", "TEXT"),
            ]:
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE execution_records ADD COLUMN {col} {col_type}")
            conn.commit()

    def _validate(self, payload: Dict[str, Any]) -> Optional[OrderExecutionRecord]:
        try:
            return OrderExecutionRecord.model_validate(payload)
        except ValidationError:
            if self.live_mode:
                raise
            logger.exception("Invalid execution diagnostics payload; skipping record")
            return None

    def _to_row(self, record: OrderExecutionRecord) -> Dict[str, Any]:
        data = record.model_dump(mode="json")
        data["is_partial_fill"] = 1 if bool(data.get("is_partial_fill")) else 0
        data["spread_crossed"] = None if data.get("spread_crossed") is None else (1 if data["spread_crossed"] else 0)
        return data

    def _upsert(self, record: OrderExecutionRecord) -> None:
        row = self._to_row(record)
        now_iso = datetime.now(UTC).isoformat()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO execution_records (
                        run_id, order_id, trade_id, market_id, market_slug, side, order_type,
                        run_tag,
                        qty_shares, intended_limit_price, time_in_force,
                        whale_signal_ts, whale_entry_ref_price, whale_ref_type,
                        our_decision_ts, order_sent_ts, exchange_ack_ts, fill_ts,
                        best_bid, best_ask, mid_price, spread_abs, spread_pct,
                        depth_bid_1, depth_ask_1, depth_bid_2, depth_ask_2, last_trade_price,
                        fill_price, entry_price_source, current_price_source, exit_price_source, fill_price_source,
                        filled_shares, fees_usd, is_partial_fill, fill_count,
                        latency_ms, quote_slippage_pct, half_spread_pct, baseline_slippage_pct,
                        spread_crossed, impact_proxy_pct, liquidity_tier, created_at, updated_at
                    ) VALUES (
                        :run_id, :order_id, :trade_id, :market_id, :market_slug, :side, :order_type,
                        :run_tag,
                        :qty_shares, :intended_limit_price, :time_in_force,
                        :whale_signal_ts, :whale_entry_ref_price, :whale_ref_type,
                        :our_decision_ts, :order_sent_ts, :exchange_ack_ts, :fill_ts,
                        :best_bid, :best_ask, :mid_price, :spread_abs, :spread_pct,
                        :depth_bid_1, :depth_ask_1, :depth_bid_2, :depth_ask_2, :last_trade_price,
                        :fill_price, :entry_price_source, :current_price_source, :exit_price_source, :fill_price_source,
                        :filled_shares, :fees_usd, :is_partial_fill, :fill_count,
                        :latency_ms, :quote_slippage_pct, :half_spread_pct, :baseline_slippage_pct,
                        :spread_crossed, :impact_proxy_pct, :liquidity_tier, :created_at, :updated_at
                    )
                    ON CONFLICT(run_id, order_id) DO UPDATE SET
                        trade_id=excluded.trade_id,
                        market_slug=excluded.market_slug,
                        run_tag=excluded.run_tag,
                        qty_shares=excluded.qty_shares,
                        intended_limit_price=excluded.intended_limit_price,
                        time_in_force=excluded.time_in_force,
                        whale_signal_ts=excluded.whale_signal_ts,
                        whale_entry_ref_price=excluded.whale_entry_ref_price,
                        whale_ref_type=excluded.whale_ref_type,
                        our_decision_ts=excluded.our_decision_ts,
                        order_sent_ts=excluded.order_sent_ts,
                        exchange_ack_ts=excluded.exchange_ack_ts,
                        fill_ts=excluded.fill_ts,
                        best_bid=excluded.best_bid,
                        best_ask=excluded.best_ask,
                        mid_price=excluded.mid_price,
                        spread_abs=excluded.spread_abs,
                        spread_pct=excluded.spread_pct,
                        depth_bid_1=excluded.depth_bid_1,
                        depth_ask_1=excluded.depth_ask_1,
                        depth_bid_2=excluded.depth_bid_2,
                        depth_ask_2=excluded.depth_ask_2,
                        last_trade_price=excluded.last_trade_price,
                        fill_price=excluded.fill_price,
                        entry_price_source=excluded.entry_price_source,
                        current_price_source=excluded.current_price_source,
                        exit_price_source=excluded.exit_price_source,
                        fill_price_source=excluded.fill_price_source,
                        filled_shares=excluded.filled_shares,
                        fees_usd=excluded.fees_usd,
                        is_partial_fill=excluded.is_partial_fill,
                        fill_count=excluded.fill_count,
                        latency_ms=excluded.latency_ms,
                        quote_slippage_pct=excluded.quote_slippage_pct,
                        half_spread_pct=excluded.half_spread_pct,
                        baseline_slippage_pct=excluded.baseline_slippage_pct,
                        spread_crossed=excluded.spread_crossed,
                        impact_proxy_pct=excluded.impact_proxy_pct,
                        liquidity_tier=excluded.liquidity_tier,
                        updated_at=excluded.updated_at
                    """,
                    {
                        **row,
                        "created_at": now_iso,
                        "updated_at": now_iso,
                    },
                )
                conn.commit()

    def get_debug_info(self) -> Dict[str, Any]:
        """Return debug metadata for troubleshooting empty exports."""
        db_abs_path = str(Path(self.db_path).resolve())
        info: Dict[str, Any] = {
            "db_path": db_abs_path,
            "table_exists": False,
            "row_count": 0,
            "sample_rows": [],
        }
        with self._connect() as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='execution_records'"
            )
            table_exists = cursor.fetchone() is not None
            info["table_exists"] = table_exists
            if not table_exists:
                return info

            row_count = conn.execute("SELECT COUNT(*) FROM execution_records").fetchone()[0]
            info["row_count"] = int(row_count or 0)
            if row_count:
                conn.row_factory = sqlite3.Row
                sample = conn.execute(
                    """SELECT run_tag, order_id, market_id, whale_signal_ts, order_sent_ts, fill_ts
                       FROM execution_records
                       ORDER BY updated_at DESC
                       LIMIT 3"""
                ).fetchall()
                info["sample_rows"] = [dict(row) for row in sample]
        return info

    def record_order_sent(self, payload: Dict[str, Any]) -> Optional[OrderExecutionRecord]:
        record = self._validate(payload)
        if record is None:
            return None
        self._upsert(record)
        return record

    def record_fill(self, payload: Dict[str, Any]) -> Optional[OrderExecutionRecord]:
        record = self._validate(payload)
        if record is None:
            return None
        self._upsert(record)
        return record

    def get_recent(
        self,
        limit: int = 500,
        run_id: Optional[str] = None,
        run_tag: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM execution_records"
        params: List[Any] = []
        clauses: List[str] = []
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if run_tag:
            clauses.append("run_tag = ?")
            params.append(run_tag)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = [dict(row) for row in conn.execute(query, params).fetchall()]
        for row in rows:
            row["is_partial_fill"] = bool(row.get("is_partial_fill"))
            if row.get("spread_crossed") is not None:
                row["spread_crossed"] = bool(row["spread_crossed"])
        return rows

    def fetch_records(
        self,
        run_id: Optional[str] = None,
        run_tag: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM execution_records"
        params: List[Any] = []
        clauses: List[str] = []
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if run_tag:
            clauses.append("run_tag = ?")
            params.append(run_tag)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at ASC"
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = [dict(row) for row in conn.execute(query, params).fetchall()]
        for row in rows:
            row["is_partial_fill"] = bool(row.get("is_partial_fill"))
            if row.get("spread_crossed") is not None:
                row["spread_crossed"] = bool(row["spread_crossed"])
        return rows

    def export_slippage_csv(
        self,
        output_path: Optional[str] = None,
        run_id: Optional[str] = None,
        run_tag: Optional[str] = None,
        only_filled: bool = True,
        require_fill_source: bool = True,
        require_snapshot: bool = True,
        return_stats: bool = False,
    ) -> Any:
        path = output_path or self.csv_path
        query = "SELECT * FROM execution_records"
        params: List[Any] = []
        clauses: List[str] = []
        if run_id:
            clauses.append("run_id = ?")
            params.append(run_id)
        if run_tag:
            clauses.append("run_tag = ?")
            params.append(run_tag)
        if only_filled:
            clauses.append("fill_price IS NOT NULL")
        if require_fill_source:
            clauses.append("fill_price_source = 'fill'")
        if require_snapshot:
            clauses.append("mid_price IS NOT NULL AND best_bid IS NOT NULL AND best_ask IS NOT NULL")
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at ASC"

        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = [dict(row) for row in conn.execute(query, params).fetchall()]

        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SLIPPAGE_EXPORT_COLUMNS)
            writer.writeheader()
            for row in rows:
                out_row = {key: row.get(key) for key in SLIPPAGE_EXPORT_COLUMNS}
                out_row["is_partial_fill"] = bool(out_row.get("is_partial_fill"))
                if out_row.get("spread_crossed") is not None:
                    out_row["spread_crossed"] = bool(out_row.get("spread_crossed"))
                writer.writerow(out_row)
        if not return_stats:
            return str(output)

        # Build exclusion reasons for transparency.
        exclusion_stats = {"not_filled": 0, "non_fill_source": 0, "missing_snapshot": 0}
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            all_rows = [dict(r) for r in conn.execute("SELECT * FROM execution_records").fetchall()]
        for row in all_rows:
            if run_id and row.get("run_id") != run_id:
                continue
            if run_tag and row.get("run_tag") != run_tag:
                continue
            if row.get("fill_price") is None:
                exclusion_stats["not_filled"] += 1
                continue
            if row.get("fill_price_source") != "fill":
                exclusion_stats["non_fill_source"] += 1
                continue
            if row.get("mid_price") is None or row.get("best_bid") is None or row.get("best_ask") is None:
                exclusion_stats["missing_snapshot"] += 1
        return str(output), exclusion_stats
