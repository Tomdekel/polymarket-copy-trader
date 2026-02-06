"""Reward ledger for market making experiments."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional


class RewardLedger:
    def __init__(self, db_path: str = "trades.db"):
        self.db_path = db_path
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_table(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_making_rewards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    run_tag TEXT NOT NULL,
                    market_id TEXT,
                    reward_usd REAL NOT NULL,
                    reward_source TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def add_reward(
        self,
        *,
        run_id: str,
        run_tag: str,
        market_id: Optional[str],
        reward_usd: float,
        reward_source: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO market_making_rewards
                   (run_id, run_tag, market_id, reward_usd, reward_source, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (run_id, run_tag, market_id, reward_usd, reward_source, datetime.utcnow().isoformat()),
            )
            conn.commit()

    def get_rewards(self, *, run_id: str, run_tag: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM market_making_rewards WHERE run_id = ? AND run_tag = ?",
                    (run_id, run_tag),
                ).fetchall()
            ]
        return rows

    def sum_rewards(self, *, run_id: str, run_tag: str) -> float:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(reward_usd), 0) FROM market_making_rewards WHERE run_id = ? AND run_tag = ?",
                (run_id, run_tag),
            ).fetchone()
        return float(row[0] or 0.0)
