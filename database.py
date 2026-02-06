"""SQLite database for trade history."""
import logging
import re
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from pnl import (
    compute_shares,
    compute_realized_pnl,
    compute_unrealized_pnl,
    compute_proceeds,
    reconcile_trade_ledger,
    validate_trade_field_semantics,
    assert_price_probability,
    assert_shares_consistent,
)

# Maximum allowed length for string fields
MAX_MARKET_ID_LENGTH = 256
MAX_WALLET_ADDRESS_LENGTH = 42
logger = logging.getLogger(__name__)


@dataclass
class Trade:
    id: int
    timestamp: datetime
    market: str
    side: str
    size: float
    price: float
    target_wallet: str
    pnl: Optional[float]
    status: str


def _validate_market_id(market: str) -> str:
    """Validate and sanitize market identifier."""
    if not market or len(market) > MAX_MARKET_ID_LENGTH:
        raise ValueError(f"Invalid market identifier: length must be 1-{MAX_MARKET_ID_LENGTH}")
    # Allow alphanumeric, hyphens, underscores, and 0x prefix for addresses
    if not re.match(r'^[a-zA-Z0-9_\-x]+$', market):
        raise ValueError(f"Market contains invalid characters: {market[:50]}")
    return market


def _validate_side(side: str) -> str:
    """Validate trade side."""
    if side.upper() not in ("BUY", "SELL"):
        raise ValueError(f"Invalid trade side: {side}")
    return side.upper()


class Database:
    """Thread-safe SQLite database for trade history."""

    def __init__(self, db_path: str = "trades.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        return self._local.conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                market TEXT NOT NULL,
                side TEXT NOT NULL,
                size REAL NOT NULL,
                price REAL NOT NULL,
                target_wallet TEXT NOT NULL,
                pnl REAL,
                status TEXT DEFAULT 'open'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id INTEGER PRIMARY KEY,
                total_value REAL DEFAULT 0,
                cash REAL DEFAULT 0,
                initial_budget REAL DEFAULT 0,
                pnl_24h REAL DEFAULT 0,
                pnl_total REAL DEFAULT 0,
                updated_at TEXT,
                session_started TEXT
            )
        """)
        # Add session_started column if it doesn't exist (for existing databases)
        try:
            conn.execute("ALTER TABLE portfolio ADD COLUMN session_started TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add sell_price column if it doesn't exist (for tracking exit price)
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN sell_price REAL")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add closed_at column if it doesn't exist (for tracking when trade was closed)
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN closed_at TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add current_price column if it doesn't exist (for real-time P&L tracking)
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN current_price REAL")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add market_slug column if it doesn't exist (for readable market names)
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN market_slug TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add outcome column if it doesn't exist (YES/NO for position side)
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN outcome TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add accounting columns if they don't exist
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN shares REAL")
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN current_value REAL")
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN proceeds REAL")
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN realized_pnl REAL")
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN unrealized_pnl REAL")
        except sqlite3.OperationalError:
            pass  # Column already exists
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN entry_price_source TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN current_price_source TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN exit_price_source TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN fill_price_source TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN run_id TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE trades ADD COLUMN run_tag TEXT")
        except sqlite3.OperationalError:
            pass

        # Create pnl_history table for time-series comparison
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pnl_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                our_pnl_pct REAL,
                whale_pnl_pct REAL,
                our_total_invested REAL,
                whale_total_invested REAL
            )
        """)

        # Create indexes for frequently queried columns
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_market_status ON trades(market, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pnl_history_timestamp ON pnl_history(timestamp)")
        # Closed trades use sell_price as exit_price; keep current_price reserved for open trades.
        conn.execute("UPDATE trades SET current_price = NULL WHERE status = 'closed'")
        conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        with self._lock:
            if hasattr(self._local, 'conn') and self._local.conn is not None:
                self._local.conn.close()
                self._local.conn = None
    
    def initialize_portfolio(self, initial_budget: float, session_started: Optional[str] = None) -> None:
        """Initialize portfolio with starting budget.

        Args:
            initial_budget: Starting budget amount
            session_started: ISO timestamp of session start (defaults to now)

        Raises:
            ValueError: If session_started is not a valid ISO timestamp
        """
        with self._lock:
            conn = self._get_conn()
            if session_started is None:
                session_started = datetime.now().isoformat()
            else:
                # Validate ISO timestamp format
                try:
                    datetime.fromisoformat(session_started)
                except ValueError:
                    raise ValueError(f"Invalid session_started timestamp format: {session_started}")
            conn.execute(
                """INSERT OR REPLACE INTO portfolio
                   (id, total_value, cash, initial_budget, pnl_24h, pnl_total, updated_at, session_started)
                   VALUES (1, ?, ?, ?, 0, 0, ?, ?)""",
                (initial_budget, initial_budget, initial_budget, datetime.now().isoformat(), session_started)
            )
            conn.commit()

    def get_cash_balance(self) -> float:
        """Get current available cash balance."""
        conn = self._get_conn()
        cursor = conn.execute("SELECT cash FROM portfolio WHERE id = 1")
        row = cursor.fetchone()
        return row[0] if row else 0.0

    def get_portfolio_stats(self) -> Dict[str, Any]:
        """Get full portfolio statistics."""
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM portfolio WHERE id = 1")
        row = cursor.fetchone()
        conn.row_factory = None
        if row:
            return dict(row)
        return {"total_value": 0, "cash": 0, "initial_budget": 0, "pnl_24h": 0, "pnl_total": 0}

    def add_trade(self, market: str, side: str, size: float,
                  price: float, target_wallet: str, market_slug: str = "",
                  outcome: str = "", entry_price_source: str = "unknown",
                  current_price_source: str = "unknown",
                  run_id: Optional[str] = None,
                  run_tag: Optional[str] = None) -> int:
        """Add a trade and update cash balance accordingly.

        Args:
            market: Market identifier (validated for safe characters)
            side: Trade side (BUY or SELL)
            size: Trade size
            price: Trade price
            target_wallet: Target wallet address being copied
            market_slug: Human-readable market name/slug (optional)
            outcome: Position outcome (YES or NO, from target position)

        Returns:
            Trade ID

        Raises:
            ValueError: If inputs fail validation
        """
        # Validate inputs
        market = _validate_market_id(market)
        side = _validate_side(side)

        if size <= 0:
            raise ValueError("Trade size must be positive")
        if price <= 0:
            raise ValueError("Trade price must be positive")
        assert_price_probability(float(price), "entry_price")
        if len(target_wallet) > MAX_WALLET_ADDRESS_LENGTH:
            raise ValueError("Invalid target wallet address")

        with self._lock:
            conn = self._get_conn()
            cursor = conn.execute(
                """INSERT INTO trades
                   (timestamp, market, side, size, price, target_wallet, market_slug, outcome,
                    shares, current_price, current_value, proceeds, realized_pnl, unrealized_pnl, pnl, status,
                    entry_price_source, current_price_source, exit_price_source, fill_price_source, run_id, run_tag)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, NULL, NULL, ?, ?)""",
                (
                    datetime.now().isoformat(),
                    market,
                    side,
                    size,
                    price,
                    target_wallet,
                    market_slug,
                    outcome,
                    compute_shares(size, price),
                    price,
                    size,  # initial current value equals cost basis at entry
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    entry_price_source,
                    current_price_source,
                    run_id,
                    run_tag,
                )
            )
            trade_id = cursor.lastrowid

            # Update cash: subtract for BUY, add for SELL
            # Note: 'size' is USD invested, not number of shares
            if side == "BUY":
                conn.execute("UPDATE portfolio SET cash = cash - ? WHERE id = 1", (size,))
            elif side == "SELL":
                # For SELL, we'd be receiving USD back (but this path is rarely used
                # since we usually close positions via close_trade())
                conn.execute("UPDATE portfolio SET cash = cash + ? WHERE id = 1", (size,))

            conn.commit()
            return trade_id

    def update_trade_pnl(self, trade_id: int, current_price: float, current_price_source: str = "mark") -> float:
        """Update PnL and current price for a specific trade.

        Note: 'size' in the database is USD invested, not number of shares.
        Uses the authoritative pnl module for calculations.

        Args:
            trade_id: The trade ID to update
            current_price: Current market price

        Returns:
            Calculated P&L value
        """
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT side, size, price, shares, status, pnl FROM trades WHERE id = ?", (trade_id,)
        )
        row = cursor.fetchone()
        conn.row_factory = None

        if not row:
            return 0.0

        if (row["status"] or "open").lower() != "open":
            return float(row["pnl"] or 0.0)
        assert_price_probability(float(current_price), "current_price")

        entry_price = row["price"]
        size = row["size"]  # USD invested, not shares
        side = row["side"]

        if entry_price <= 0:
            return 0.0

        shares = float(row["shares"] or 0.0) or compute_shares(size, entry_price)
        assert_shares_consistent(
            shares=shares,
            entry_price=float(entry_price),
            cost_basis_usd=float(size),
        )

        # For BUY, profit if price goes up; for SELL (short), profit if price goes down
        if side.upper() == "BUY":
            unrealized_pnl = compute_unrealized_pnl(shares, entry_price, current_price)
        else:
            # For short positions, profit when price goes down
            unrealized_pnl = shares * (entry_price - current_price)

        current_value = shares * current_price

        conn.execute(
            """UPDATE trades
               SET shares = ?, current_price = ?, current_value = ?,
                   unrealized_pnl = ?, realized_pnl = COALESCE(realized_pnl, 0), pnl = ?, current_price_source = ?
               WHERE id = ?""",
            (shares, current_price, current_value, unrealized_pnl, unrealized_pnl, current_price_source, trade_id)
        )
        conn.commit()
        return unrealized_pnl

    def close_trade(
        self,
        trade_id: int,
        exit_price: float,
        close_size: Optional[float] = None,
        exit_price_source: str = "unknown",
        fill_price_source: str = "unknown",
    ) -> float:
        """Close a trade and realize PnL.

        Note: 'size' in the database is USD invested, not number of shares.
        Uses the authoritative pnl module for calculations.
        """
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM trades WHERE id = ? AND status = 'open'",
                (trade_id,),
            )
            row = cursor.fetchone()
            conn.row_factory = None

            if not row:
                return 0.0
            assert_price_probability(float(exit_price), "exit_price")

            entry_price = float(row["price"] or 0.0)
            size = float(row["size"] or 0.0)
            side = row["side"]
            shares_total = float(row["shares"] or 0.0) or compute_shares(size, entry_price)
            assert_shares_consistent(
                shares=shares_total,
                entry_price=entry_price,
                cost_basis_usd=size,
            )

            if entry_price <= 0 or size <= 0:
                return 0.0

            close_size = size if close_size is None else min(max(close_size, 0.0), size)
            if close_size <= 0:
                return 0.0

            shares_to_close = compute_shares(close_size, entry_price)
            if side.upper() == "BUY":
                realized_pnl = compute_realized_pnl(shares_to_close, entry_price, exit_price)
            else:
                # For short positions, profit when exit price is lower than entry
                realized_pnl = shares_to_close * (entry_price - exit_price)
            proceeds = compute_proceeds(shares_to_close, exit_price)

            closed_at = datetime.now().isoformat()
            is_full_close = abs(close_size - size) < 1e-9

            if is_full_close:
                conn.execute(
                    """UPDATE trades
                       SET status = 'closed', sell_price = ?, closed_at = ?,
                           shares = ?, current_price = NULL, current_value = ?,
                           proceeds = ?, realized_pnl = ?, unrealized_pnl = 0, pnl = ?,
                           exit_price_source = ?, fill_price_source = ?
                       WHERE id = ?""",
                    (
                        exit_price,
                        closed_at,
                        shares_total,
                        proceeds,
                        proceeds,
                        realized_pnl,
                        realized_pnl,
                        exit_price_source,
                        fill_price_source,
                        trade_id,
                    ),
                )
            else:
                # Insert a closed row for the realized portion.
                conn.execute(
                    """INSERT INTO trades
                       (timestamp, market, side, size, price, target_wallet, market_slug, outcome,
                        shares, current_price, current_value, sell_price, closed_at,
                        proceeds, realized_pnl, unrealized_pnl, pnl, status,
                        entry_price_source, current_price_source, exit_price_source, fill_price_source, run_id, run_tag)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'closed', ?, NULL, ?, ?, ?, ?)""",
                    (
                        closed_at,
                        row["market"],
                        side,
                        close_size,
                        entry_price,
                        row["target_wallet"],
                        row["market_slug"],
                        row["outcome"],
                        shares_to_close,
                        None,
                        proceeds,
                        exit_price,
                        closed_at,
                        proceeds,
                        realized_pnl,
                        0.0,
                        realized_pnl,
                        row["entry_price_source"] if "entry_price_source" in row.keys() and row["entry_price_source"] else "unknown",
                        exit_price_source,
                        fill_price_source,
                        row["run_id"] if "run_id" in row.keys() else None,
                        row["run_tag"] if "run_tag" in row.keys() else None,
                    ),
                )

                remaining_size = size - close_size
                remaining_shares = max(shares_total - shares_to_close, 0.0)
                remaining_unrealized = compute_unrealized_pnl(
                    remaining_shares, entry_price, exit_price
                )
                remaining_current_value = remaining_shares * exit_price
                conn.execute(
                    """UPDATE trades
                       SET size = ?, shares = ?, current_price = ?, current_value = ?,
                           unrealized_pnl = ?, pnl = ?, proceeds = COALESCE(proceeds, 0),
                           realized_pnl = COALESCE(realized_pnl, 0), current_price_source = ?
                       WHERE id = ?""",
                    (
                        remaining_size,
                        remaining_shares,
                        exit_price,
                        remaining_current_value,
                        remaining_unrealized,
                        remaining_unrealized,
                        exit_price_source,
                        trade_id,
                    ),
                )

            conn.execute(
                "UPDATE portfolio SET cash = cash + ?, pnl_total = pnl_total + ? WHERE id = 1",
                (proceeds, realized_pnl),
            )
            conn.commit()
            return realized_pnl

    def get_open_positions(self) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM trades WHERE status = 'open' ORDER BY timestamp DESC"
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.row_factory = None
        return rows

    def get_position_by_market(self, market: str) -> Optional[Dict[str, Any]]:
        """Get open position for a specific market."""
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM trades WHERE market = ? AND status = 'open' ORDER BY timestamp DESC LIMIT 1",
            (market,)
        )
        row = cursor.fetchone()
        conn.row_factory = None
        return dict(row) if row else None

    def update_portfolio(self, total_value: float, cash: float,
                         pnl_24h: float) -> None:
        """Update portfolio stats except pnl_total (which is updated by close_trade()).

        Args:
            total_value: Current total portfolio value
            cash: Available cash balance
            pnl_24h: P&L in the last 24 hours
        """
        conn = self._get_conn()
        # Note: pnl_total is NOT updated here - it's managed exclusively by close_trade()
        # to avoid race conditions and double-counting
        conn.execute(
            """UPDATE portfolio SET total_value = ?, cash = ?, pnl_24h = ?, updated_at = ?
               WHERE id = 1""",
            (total_value, cash, pnl_24h, datetime.now().isoformat())
        )
        conn.commit()

    def calculate_24h_pnl(self) -> float:
        """Calculate realized PnL for closed trades in the last 24 hours."""
        conn = self._get_conn()
        cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
        cursor = conn.execute(
            """SELECT COALESCE(SUM(COALESCE(realized_pnl, pnl)), 0)
               FROM trades
               WHERE status = 'closed'
                 AND COALESCE(closed_at, timestamp) > ?""",
            (cutoff,)
        )
        row = cursor.fetchone()
        return row[0] if row else 0.0

    def calculate_7d_pnl(self) -> float:
        """Calculate realized PnL for closed trades in the last 7 days."""
        conn = self._get_conn()
        cutoff = (datetime.now() - timedelta(days=7)).isoformat()
        cursor = conn.execute(
            """SELECT COALESCE(SUM(COALESCE(realized_pnl, pnl)), 0)
               FROM trades
               WHERE status = 'closed'
                 AND COALESCE(closed_at, timestamp) > ?""",
            (cutoff,),
        )
        row = cursor.fetchone()
        return row[0] if row else 0.0

    def get_recent_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades for dashboard display.

        Args:
            limit: Maximum number of trades to return

        Returns:
            List of trade dictionaries ordered by timestamp descending
        """
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.row_factory = None
        return rows

    def get_all_trades(self) -> List[Dict[str, Any]]:
        """Get all trades ordered by timestamp ascending."""
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("SELECT * FROM trades ORDER BY timestamp ASC, id ASC")
        rows = [dict(row) for row in cursor.fetchall()]
        conn.row_factory = None
        return rows

    def get_all_trades_normalized(self) -> List[Dict[str, Any]]:
        """Get all trades with explicit accounting field names."""
        normalized: List[Dict[str, Any]] = []
        for trade in self.get_all_trades():
            entry_price = float(trade.get("price") or 0.0)
            size_usd = float(trade.get("size") or 0.0)
            shares = float(trade.get("shares") or 0.0) or compute_shares(size_usd, entry_price)
            normalized.append({
                "id": trade.get("id"),
                "timestamp": trade.get("timestamp"),
                "market": trade.get("market"),
                "status": trade.get("status"),
                "side": trade.get("side"),
                "outcome": trade.get("outcome"),
                "shares": shares,
                "entry_price": entry_price,
                "exit_price": trade.get("sell_price"),
                "current_price": trade.get("current_price"),
                "entry_price_source": trade.get("entry_price_source"),
                "current_price_source": trade.get("current_price_source"),
                "exit_price_source": trade.get("exit_price_source"),
                "fill_price_source": trade.get("fill_price_source"),
                "cost_basis_usd": size_usd,
                "proceeds_usd": float(trade.get("proceeds") or 0.0),
                "realized_pnl_usd": float(trade.get("realized_pnl") or 0.0),
                "unrealized_pnl_usd": float(trade.get("unrealized_pnl") or 0.0),
            })
        return normalized

    def reconcile_portfolio(self, starting_equity: Optional[float] = None) -> Dict[str, Any]:
        """Reconcile portfolio totals from ledger rows."""
        cash = self.get_cash_balance()
        trades = self.get_all_trades()
        return reconcile_trade_ledger(cash=cash, trades=trades, starting_equity=starting_equity)

    def validate_trade_integrity(self, eps: float = 1e-6) -> List[str]:
        """Validate core accounting identities on trade rows."""
        issues: List[str] = []
        open_value_total = 0.0
        allowed_sources = {"fill", "quote", "mark", "whale_ref", "placeholder", "unknown"}
        for trade in self.get_all_trades():
            trade_id = trade.get("id")
            status = (trade.get("status") or "open").lower()
            issues.extend(validate_trade_field_semantics(trade, eps=eps))
            for source_field in ("entry_price_source", "current_price_source", "exit_price_source", "fill_price_source"):
                source_value = trade.get(source_field)
                if source_value is not None and source_value not in allowed_sources:
                    issues.append(f"trade_id={trade_id} invalid {source_field}={source_value}")
            size = float(trade.get("size") or 0.0)
            entry_price = float(trade.get("price") or 0.0)
            shares = float(trade.get("shares") or 0.0)
            if shares <= 0 and entry_price > 0:
                shares = compute_shares(size, entry_price)

            expected_cost_basis = shares * entry_price
            if entry_price > 0 and abs(size - expected_cost_basis) > eps:
                issues.append(
                    f"trade_id={trade_id} cost_basis mismatch: size={size:.8f}, shares*entry={expected_cost_basis:.8f}"
                )

            if status == "open":
                current_price = trade.get("current_price")
                if current_price is not None:
                    current_price = float(current_price)
                    expected_current = shares * current_price
                    stored_current = float(trade.get("current_value") or 0.0)
                    open_value_total += expected_current
                    if abs(stored_current - expected_current) > eps:
                        issues.append(
                            f"trade_id={trade_id} current_value mismatch: stored={stored_current:.8f}, expected={expected_current:.8f}"
                        )
                    expected_unrealized = compute_unrealized_pnl(shares, entry_price, current_price)
                    stored_unrealized = float(trade.get("unrealized_pnl") or trade.get("pnl") or 0.0)
                    if abs(stored_unrealized - expected_unrealized) > eps:
                        issues.append(
                            f"trade_id={trade_id} unrealized mismatch: stored={stored_unrealized:.8f}, expected={expected_unrealized:.8f}"
                        )
            else:
                exit_price = trade.get("sell_price")
                if exit_price is not None:
                    exit_price = float(exit_price)
                    expected_proceeds = shares * exit_price
                    stored_proceeds = float(trade.get("proceeds") or 0.0)
                    if abs(stored_proceeds - expected_proceeds) > eps:
                        issues.append(
                            f"trade_id={trade_id} proceeds mismatch: stored={stored_proceeds:.8f}, expected={expected_proceeds:.8f}"
                        )
                    expected_realized = compute_realized_pnl(shares, entry_price, exit_price)
                    stored_realized = float(trade.get("realized_pnl") or trade.get("pnl") or 0.0)
                    if abs(stored_realized - expected_realized) > eps:
                        issues.append(
                            f"trade_id={trade_id} realized mismatch: stored={stored_realized:.8f}, expected={expected_realized:.8f}"
                        )
        stats = self.get_portfolio_stats()
        portfolio_current_value = float(stats.get("total_value") or 0.0)
        cash = float(stats.get("cash") or 0.0)
        expected_total = cash + open_value_total
        if abs(portfolio_current_value - expected_total) > eps:
            issues.append(
                f"portfolio total mismatch: stored={portfolio_current_value:.8f}, expected={expected_total:.8f}"
            )
        return issues

    def run_reconciliation_gate(self, mode: str, eps: float = 1e-6) -> None:
        """Enforce last-mile accounting invariants before trading/reporting."""
        issues = self.validate_trade_integrity(eps=eps)
        mode_normalized = (mode or "").lower()

        if mode_normalized == "live":
            for trade in self.get_all_trades():
                status = (trade.get("status") or "open").lower()
                if status != "closed":
                    continue
                trade_id = trade.get("id")
                if trade.get("sell_price") is None:
                    issues.append(f"trade_id={trade_id} live invariant: closed trade missing exit_price")
                if trade.get("exit_price_source") != "fill":
                    issues.append(
                        f"trade_id={trade_id} live invariant: exit_price_source must be 'fill', got {trade.get('exit_price_source')}"
                    )
                if trade.get("fill_price_source") != "fill":
                    issues.append(
                        f"trade_id={trade_id} live invariant: fill_price_source must be 'fill', got {trade.get('fill_price_source')}"
                    )

        if not issues:
            return

        for issue in issues:
            logger.error("Reconciliation gate failed: %s", issue)

        if mode_normalized == "live":
            raise RuntimeError("Live reconciliation gate failed; halting trading")
        if mode_normalized in {"backtest", "dry_run"}:
            raise AssertionError("Backtest reconciliation gate failed")
        raise RuntimeError("Reconciliation gate failed")

    def get_session_start_time(self) -> Optional[str]:
        """Get session start timestamp from portfolio or first trade.

        Returns:
            ISO timestamp string or None if no data
        """
        conn = self._get_conn()
        # First try to get from portfolio table
        cursor = conn.execute("SELECT session_started FROM portfolio WHERE id = 1")
        row = cursor.fetchone()
        if row and row[0]:
            return row[0]

        # Fall back to first trade timestamp
        cursor = conn.execute("SELECT MIN(timestamp) FROM trades")
        row = cursor.fetchone()
        return row[0] if row else None

    def get_trade_statistics(self) -> Dict[str, Any]:
        """Get comprehensive trade statistics.

        Returns:
            Dictionary with trade statistics including:
            - total_trades: count of all trades
            - open_trades: count where status='open'
            - closed_trades: count where status='closed' (we exited or resolved)
            - total_buys: count where side='BUY'
            - total_sells: count where side='SELL'
            - winning_trades: count where pnl > 0 and status='closed'
            - losing_trades: count where pnl < 0 and status='closed'
            - avg_trade_size: average of size
            - avg_win: average pnl where pnl > 0
            - avg_loss: average pnl where pnl < 0
            - largest_win: max pnl
            - largest_loss: min pnl
            - win_rate: winning_trades / (winning + losing) * 100
        """
        conn = self._get_conn()

        # Single optimized query to get all statistics
        cursor = conn.execute("""
            SELECT
                COUNT(*) as total_trades,
                COUNT(CASE WHEN status = 'open' THEN 1 END) as open_trades,
                COUNT(CASE WHEN status = 'closed' THEN 1 END) as closed_trades,
                COUNT(CASE WHEN side = 'BUY' THEN 1 END) as total_buys,
                COUNT(CASE WHEN side = 'SELL' THEN 1 END) as total_sells,
                COUNT(CASE WHEN status = 'closed' AND COALESCE(realized_pnl, pnl) > 0 THEN 1 END) as winning_trades,
                COUNT(CASE WHEN status = 'closed' AND COALESCE(realized_pnl, pnl) < 0 THEN 1 END) as losing_trades,
                AVG(size) as avg_trade_size,
                AVG(CASE WHEN status = 'closed' AND COALESCE(realized_pnl, pnl) > 0 THEN COALESCE(realized_pnl, pnl) END) as avg_win,
                AVG(CASE WHEN status = 'closed' AND COALESCE(realized_pnl, pnl) < 0 THEN COALESCE(realized_pnl, pnl) END) as avg_loss,
                MAX(CASE WHEN status = 'closed' THEN COALESCE(realized_pnl, pnl) END) as largest_win,
                MIN(CASE WHEN status = 'closed' THEN COALESCE(realized_pnl, pnl) END) as largest_loss
            FROM trades
        """)
        row = cursor.fetchone()

        # Extract values with defaults for NULL
        total_trades = row[0] or 0
        open_trades = row[1] or 0
        closed_trades = row[2] or 0
        total_buys = row[3] or 0
        total_sells = row[4] or 0
        winning_trades = row[5] or 0
        losing_trades = row[6] or 0
        avg_trade_size = row[7] or 0.0
        avg_win = row[8] or 0.0
        avg_loss = row[9] or 0.0
        largest_win = row[10] or 0.0
        largest_loss = row[11] or 0.0

        # Win rate calculation (use all closed trades as denominator, including break-even)
        win_rate = (winning_trades / closed_trades * 100) if closed_trades > 0 else 0.0

        return {
            "total_trades": total_trades,
            "open_trades": open_trades,
            "closed_trades": closed_trades,
            "total_buys": total_buys,
            "total_sells": total_sells,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "avg_trade_size": avg_trade_size,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "largest_win": largest_win,
            "largest_loss": largest_loss,
            "win_rate": win_rate,
        }

    def record_pnl_snapshot(
        self,
        our_pnl_pct: float,
        whale_pnl_pct: float,
        our_total_invested: float = 0.0,
        whale_total_invested: float = 0.0,
    ) -> None:
        """Record a P&L snapshot for time-series comparison.

        Args:
            our_pnl_pct: Our P&L as percentage of invested capital
            whale_pnl_pct: Whale's P&L as percentage of invested capital
            our_total_invested: Our total invested amount (for context)
            whale_total_invested: Whale's total invested amount (for context)
        """
        conn = self._get_conn()
        conn.execute(
            """INSERT INTO pnl_history (timestamp, our_pnl_pct, whale_pnl_pct, our_total_invested, whale_total_invested)
               VALUES (?, ?, ?, ?, ?)""",
            (datetime.now().isoformat(), our_pnl_pct, whale_pnl_pct, our_total_invested, whale_total_invested)
        )
        conn.commit()

    def get_pnl_history(self, hours: int = 48) -> List[Dict[str, Any]]:
        """Get P&L history for the last N hours.

        Args:
            hours: Number of hours of history to return (default 48)

        Returns:
            List of snapshots with timestamp, our_pnl_pct, whale_pnl_pct
        """
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        cursor = conn.execute(
            """SELECT timestamp, our_pnl_pct, whale_pnl_pct, our_total_invested, whale_total_invested
               FROM pnl_history
               WHERE timestamp > ?
               ORDER BY timestamp ASC""",
            (cutoff,)
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.row_factory = None
        return rows

    def reset_pnl_total(self) -> float:
        """Reset corrupted pnl_total to 0.

        Use this when pnl_total has been corrupted by a bug and needs
        to be reset. Only realized P&L from closed trades should be
        accumulated in pnl_total.

        Returns:
            The old pnl_total value before reset (for logging/verification)
        """
        import logging
        logger = logging.getLogger(__name__)

        with self._lock:
            conn = self._get_conn()
            # Get the current value before reset
            cursor = conn.execute("SELECT pnl_total FROM portfolio WHERE id = 1")
            row = cursor.fetchone()
            old_value = row[0] if row else 0.0

            conn.execute("UPDATE portfolio SET pnl_total = 0 WHERE id = 1")
            conn.commit()

            logger.warning(
                f"RESET pnl_total: old_value={old_value:.2f}, new_value=0.00, "
                f"timestamp={datetime.now().isoformat()}"
            )
            return old_value

    def get_pnl_history_sampled(self, hours: int = 48, interval_hours: int = 5) -> List[Dict[str, Any]]:
        """Get P&L history sampled at regular intervals.

        Args:
            hours: Total hours of history to return (default 48)
            interval_hours: Interval between samples in hours (default 5)

        Returns:
            List of sampled snapshots, one per interval
        """
        all_history = self.get_pnl_history(hours)
        if not all_history:
            return []

        # Sample at specified intervals
        sampled = []
        last_sample_time = None

        for snapshot in all_history:
            try:
                snap_time = datetime.fromisoformat(snapshot['timestamp'])
            except (ValueError, TypeError):
                continue

            if last_sample_time is None:
                sampled.append(snapshot)
                last_sample_time = snap_time
            else:
                delta = (snap_time - last_sample_time).total_seconds() / 3600
                if delta >= interval_hours:
                    sampled.append(snapshot)
                    last_sample_time = snap_time

        # Always include the most recent snapshot
        if all_history and (not sampled or sampled[-1] != all_history[-1]):
            sampled.append(all_history[-1])

        return sampled
