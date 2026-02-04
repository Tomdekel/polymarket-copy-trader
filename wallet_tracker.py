"""Tracks target wallet positions via Polymarket Gamma API."""
import requests
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

GAMMA_API_BASE = "https://gamma-api.polymarket.com"

@dataclass
class Position:
    market: str
    market_slug: str
    outcome: str  # YES or NO
    size: float
    avg_price: float
    current_price: Optional[float]
    value: float
    pnl: float

class WalletTracker:
    def __init__(self, wallet_address: str):
        self.wallet = wallet_address
    
    def get_positions(self) -> List[Position]:
        """Fetch current positions for the target wallet."""
        url = f"{GAMMA_API_BASE}/portfolio/users/{self.wallet}/positions"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            positions = []
            for pos in data.get("positions", []):
                positions.append(Position(
                    market=pos.get("market", ""),
                    market_slug=pos.get("market_slug", ""),
                    outcome="YES" if pos.get("outcome_index") == 0 else "NO",
                    size=float(pos.get("size", 0)),
                    avg_price=float(pos.get("avg_price", 0)),
                    current_price=float(pos.get("current_price")) if pos.get("current_price") else None,
                    value=float(pos.get("value", 0)),
                    pnl=float(pos.get("pnl", 0))
                ))
            return positions
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch positions: {e}")
    
    def get_portfolio_value(self) -> float:
        """Get total portfolio value of target wallet."""
        url = f"{GAMMA_API_BASE}/portfolio/users/{self.wallet}/balance"
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return float(data.get("total_value", 0))
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch portfolio value: {e}")
    
    def get_markets(self) -> List[Dict[str, Any]]:
        """Get list of available markets."""
        url = f"{GAMMA_API_BASE}/markets"
        try:
            resp = requests.get(url, params={"active": True, "closed": False}, timeout=30)
            resp.raise_for_status()
            return resp.json().get("markets", [])
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch markets: {e}")
