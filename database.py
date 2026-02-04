"""SQLite database for trade history."""
import re
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

# Maximum allowed length for string fields
MAX_MARKET_ID_LENGTH = 256
MAX_WALLET_ADDRESS_LENGTH = 42


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

        # Create indexes for frequently queried columns
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_market_status ON trades(market, status)")
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
                  price: float, target_wallet: str) -> int:
        """Add a trade and update cash balance accordingly.

        Args:
            market: Market identifier (validated for safe characters)
            side: Trade side (BUY or SELL)
            size: Trade size
            price: Trade price
            target_wallet: Target wallet address being copied

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
        if price < 0:
            raise ValueError("Trade price cannot be negative")
        if len(target_wallet) > MAX_WALLET_ADDRESS_LENGTH:
            raise ValueError("Invalid target wallet address")

        with self._lock:
            conn = self._get_conn()
            cursor = conn.execute(
                """INSERT INTO trades (timestamp, market, side, size, price, target_wallet)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (datetime.now().isoformat(), market, side, size, price, target_wallet)
            )
            trade_id = cursor.lastrowid

            # Update cash: subtract for BUY, add for SELL
            cost = size * price if price > 0 else size
            if side == "BUY":
                conn.execute("UPDATE portfolio SET cash = cash - ? WHERE id = 1", (cost,))
            elif side == "SELL":
                conn.execute("UPDATE portfolio SET cash = cash + ? WHERE id = 1", (cost,))

            conn.commit()
            return trade_id

    def update_trade_pnl(self, trade_id: int, current_price: float) -> float:
        """Update PnL for a specific trade based on current price."""
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT side, size, price FROM trades WHERE id = ?", (trade_id,)
        )
        row = cursor.fetchone()
        conn.row_factory = None

        if not row:
            return 0.0

        entry_price = row["price"]
        size = row["size"]
        side = row["side"]

        if entry_price == 0:
            return 0.0

        # Calculate PnL: for BUY, profit if price goes up; for SELL (short), profit if price goes down
        if side.upper() == "BUY":
            pnl = (current_price - entry_price) * size
        else:
            pnl = (entry_price - current_price) * size

        conn.execute("UPDATE trades SET pnl = ? WHERE id = ?", (pnl, trade_id))
        conn.commit()
        return pnl

    def close_trade(self, trade_id: int, exit_price: float) -> float:
        """Close a trade and realize PnL."""
        conn = self._get_conn()
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT side, size, price FROM trades WHERE id = ?", (trade_id,)
        )
        row = cursor.fetchone()
        conn.row_factory = None

        if not row:
            return 0.0

        entry_price = row["price"]
        size = row["size"]
        side = row["side"]

        # Calculate final PnL
        if side.upper() == "BUY":
            pnl = (exit_price - entry_price) * size
            # Return proceeds from selling
            proceeds = size * exit_price
        else:
            pnl = (entry_price - exit_price) * size
            proceeds = size * exit_price

        # Update trade status and PnL
        conn.execute(
            "UPDATE trades SET status = 'closed', pnl = ? WHERE id = ?",
            (pnl, trade_id)
        )

        # Add proceeds back to cash and update total PnL
        conn.execute(
            "UPDATE portfolio SET cash = cash + ?, pnl_total = pnl_total + ? WHERE id = 1",
            (proceeds, pnl)
        )
        conn.commit()
        return pnl

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
                         pnl_24h: float, pnl_total: float) -> None:
        conn = self._get_conn()
        conn.execute(
            """UPDATE portfolio SET total_value = ?, cash = ?, pnl_24h = ?, pnl_total = ?, updated_at = ?
               WHERE id = 1""",
            (total_value, cash, pnl_24h, pnl_total, datetime.now().isoformat())
        )
        conn.commit()

    def calculate_24h_pnl(self) -> float:
        """Calculate PnL for trades in the last 24 hours."""
        conn = self._get_conn()
        cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
        cursor = conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE timestamp > ? AND pnl IS NOT NULL",
            (cutoff,)
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
                COUNT(CASE WHEN status = 'closed' AND pnl > 0 THEN 1 END) as winning_trades,
                COUNT(CASE WHEN status = 'closed' AND pnl < 0 THEN 1 END) as losing_trades,
                AVG(size) as avg_trade_size,
                AVG(CASE WHEN status = 'closed' AND pnl > 0 THEN pnl END) as avg_win,
                AVG(CASE WHEN status = 'closed' AND pnl < 0 THEN pnl END) as avg_loss,
                MAX(CASE WHEN status = 'closed' THEN pnl END) as largest_win,
                MIN(CASE WHEN status = 'closed' THEN pnl END) as largest_loss
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

        # Win rate calculation
        total_closed_with_pnl = winning_trades + losing_trades
        win_rate = (winning_trades / total_closed_with_pnl * 100) if total_closed_with_pnl > 0 else 0.0

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
