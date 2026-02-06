"""Tracks target wallet positions via Polymarket Gamma API."""
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from utils import validate_wallet_address
from api_client import GammaAPIClient, APIError

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
    liquidity: Optional[float] = None
    timestamp: Optional[str] = None

class WalletTracker:
    def __init__(self, wallet_address: str, api_client: Optional[GammaAPIClient] = None):
        self.wallet = validate_wallet_address(wallet_address)
        self._client = api_client or GammaAPIClient()

    def close(self) -> None:
        """Close API client resources."""
        self._client.close()
    
    def get_positions(self) -> List[Position]:
        """Fetch current positions for the target wallet."""
        try:
            data = self._client.get_positions(self.wallet)

            positions = []
            for pos in data.get("positions", []):
                # Handle both old and new API formats
                # New format: conditionId, slug, title, outcomeIndex, curPrice, avgPrice, currentValue, cashPnl
                # Old format: market, market_slug, outcome_index, current_price, avg_price, value, pnl
                market = pos.get("conditionId") or pos.get("market", "")
                market_slug = pos.get("slug") or pos.get("title") or pos.get("market_slug", "")

                # Determine outcome: new API uses "outcome" field directly or outcomeIndex
                outcome_str = pos.get("outcome", "")
                if outcome_str:
                    outcome = outcome_str.upper() if outcome_str.lower() in ("yes", "no") else outcome_str
                else:
                    outcome = "YES" if pos.get("outcomeIndex", pos.get("outcome_index", 0)) == 0 else "NO"

                current_price = pos.get("curPrice") or pos.get("current_price")

                positions.append(Position(
                    market=market,
                    market_slug=market_slug,
                    outcome=outcome,
                    size=float(pos.get("size", 0)),
                    avg_price=float(pos.get("avgPrice", pos.get("avg_price", 0))),
                    current_price=float(current_price) if current_price else None,
                    value=float(pos.get("currentValue", pos.get("value", 0))),
                    pnl=float(pos.get("cashPnl", pos.get("pnl", 0))),
                    liquidity=float(pos.get("liquidity", 0)) if pos.get("liquidity") is not None else None,
                    timestamp=pos.get("timestamp") or pos.get("updatedAt"),
                ))
            return positions
        except APIError as e:
            raise RuntimeError(f"Failed to fetch positions: {e}")

    def get_portfolio_value(self) -> float:
        """Get total portfolio value of target wallet."""
        try:
            data = self._client.get_portfolio_balance(self.wallet)
            return float(data.get("balance", data.get("total_value", 0)))
        except APIError as e:
            raise RuntimeError(f"Failed to fetch portfolio value: {e}")

    def get_markets(self) -> List[Dict[str, Any]]:
        """Get list of available markets."""
        try:
            data = self._client.get_markets(active=True, closed=False)
            return data.get("markets", [])
        except APIError as e:
            raise RuntimeError(f"Failed to fetch markets: {e}")

    def get_market_price(self, market_id: str, outcome: str = "YES") -> Optional[float]:
        """Get current price for a market outcome.

        Args:
            market_id: The market identifier (condition_id or slug)
            outcome: "YES" or "NO"

        Returns:
            Current price as a float (0-1), or None if unavailable
        """
        # Try CLOB API first (more reliable)
        prices = self._client.get_market_price_clob(market_id)
        if prices:
            key = "yes" if outcome.upper() == "YES" else "no"
            return prices.get(key)

        # Fallback to data API
        try:
            data = self._client.get_market(market_id)

            # Price might be in different fields depending on API version
            if outcome.upper() == "YES":
                price = data.get("outcomePrices", [None, None])[0]
                if price is None:
                    price = data.get("yes_price") or data.get("bestBid")
            else:
                price = data.get("outcomePrices", [None, None])[1]
                if price is None:
                    price = data.get("no_price") or data.get("bestAsk")

            return float(price) if price is not None else None
        except APIError:
            return None
        except (IndexError, ValueError, TypeError):
            return None

    def get_market_snapshot(self, market_id: str, outcome: str = "YES") -> Dict[str, Optional[float]]:
        """Get best bid/ask snapshot for execution diagnostics."""
        snapshot = self._client.get_market_snapshot_clob(market_id, outcome=outcome)
        if snapshot:
            return snapshot

        price = self.get_market_price(market_id, outcome)
        if price is None:
            return {
                "best_bid": None,
                "best_ask": None,
                "mid_price": None,
                "depth_bid_1": None,
                "depth_ask_1": None,
                "last_trade_price": None,
            }
        # Conservative fallback: if we only have one price, treat it as both sides.
        return {
            "best_bid": price,
            "best_ask": price,
            "mid_price": price,
            "depth_bid_1": None,
            "depth_ask_1": None,
            "last_trade_price": price,
        }

    def get_position_current_price(self, position: Position) -> Optional[float]:
        """Get current price for an existing position."""
        if position.current_price is not None:
            return position.current_price
        return self.get_market_price(position.market, position.outcome)
