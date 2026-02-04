"""SQLite database for trade history."""
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

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

class Database:
    def __init__(self, db_path: str = "trades.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
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
                    pnl_24h REAL DEFAULT 0,
                    pnl_total REAL DEFAULT 0,
                    updated_at TEXT
                )
            """)
    
    def add_trade(self, market: str, side: str, size: float, 
                  price: float, target_wallet: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """INSERT INTO trades (timestamp, market, side, size, price, target_wallet)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (datetime.now().isoformat(), market, side, size, price, target_wallet)
            )
            return cursor.lastrowid
    
    def get_open_positions(self) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM trades WHERE status = 'open' ORDER BY timestamp DESC"
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def update_portfolio(self, total_value: float, cash: float, 
                         pnl_24h: float, pnl_total: float) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT OR REPLACE INTO portfolio 
                   (id, total_value, cash, pnl_24h, pnl_total, updated_at)
                   VALUES (1, ?, ?, ?, ?, ?)""",
                (total_value, cash, pnl_24h, pnl_total, datetime.now().isoformat())
            )
