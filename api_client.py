"""Rate-limited HTTP client for Gamma API."""
import logging
import requests
from typing import Optional, Dict, Any
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger("polymarket_copy_trader")

GAMMA_API_BASE = "https://data-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

# Default rate limit settings
DEFAULT_MAX_RETRIES = 3
DEFAULT_MIN_WAIT = 1  # seconds
DEFAULT_MAX_WAIT = 30  # seconds


class APIError(Exception):
    """Raised when API request fails after retries."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class GammaAPIClient:
    """Rate-limited client for Polymarket Gamma API."""

    def __init__(
        self,
        base_url: str = GAMMA_API_BASE,
        max_retries: int = DEFAULT_MAX_RETRIES,
        min_wait: float = DEFAULT_MIN_WAIT,
        max_wait: float = DEFAULT_MAX_WAIT,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.timeout = timeout
        self._session: Optional[requests.Session] = None

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                "Accept": "application/json",
                "User-Agent": "PolymarketCopyTrader/1.0",
            })
        return self._session

    def close(self) -> None:
        """Close the HTTP session."""
        if self._session is not None:
            self._session.close()
            self._session = None

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make an HTTP request with retry logic."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(
                multiplier=1, min=self.min_wait, max=self.max_wait
            ),
            retry=retry_if_exception_type(
                (requests.RequestException, requests.Timeout)
            ),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        def do_request():
            resp = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json()

        try:
            return do_request()
        except requests.HTTPError as e:
            status_code = e.response.status_code if e.response else None
            raise APIError(f"HTTP {status_code}: {e}", status_code=status_code)
        except requests.RequestException as e:
            raise APIError(f"Request failed after {self.max_retries} retries: {e}")

    def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make a GET request."""
        return self._make_request("GET", endpoint, params=params)

    def post(
        self,
        endpoint: str,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make a POST request."""
        return self._make_request("POST", endpoint, params=params, json=json)

    # Convenience methods for common endpoints
    def get_positions(self, wallet_address: str) -> Dict[str, Any]:
        """Fetch positions for a wallet."""
        # Data API returns a list directly, wrap in dict for consistency
        result = self.get("/positions", params={"user": wallet_address})
        if isinstance(result, list):
            return {"positions": result}
        return result

    def get_portfolio_balance(self, wallet_address: str) -> Dict[str, Any]:
        """Fetch portfolio balance for a wallet."""
        # Calculate from positions
        positions = self.get_positions(wallet_address)
        total_value = sum(p.get("currentValue", 0) for p in positions.get("positions", []))
        return {"balance": total_value}

    def get_market(self, market_id: str) -> Dict[str, Any]:
        """Fetch market details."""
        return self.get(f"/markets/{market_id}")

    def get_markets(
        self, active: bool = True, closed: bool = False
    ) -> Dict[str, Any]:
        """Fetch list of markets."""
        return self.get("/markets", params={"active": active, "closed": closed})

    def get_market_price_clob(self, condition_id: str) -> Optional[Dict[str, float]]:
        """Fetch current prices from CLOB API.

        Args:
            condition_id: The market condition ID

        Returns:
            Dict with 'yes' and 'no' prices, or None if unavailable
        """
        try:
            # CLOB API endpoint for market data
            url = f"{CLOB_API_BASE}/markets/{condition_id}"
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()

            # Extract outcome prices
            outcome_prices = data.get("outcome_prices") or data.get("outcomePrices", [])
            if outcome_prices and len(outcome_prices) >= 2:
                yes_price = float(outcome_prices[0])
                no_price = float(outcome_prices[1])
                # Validate price range (0-1 for prediction markets)
                if not (0 <= yes_price <= 1 and 0 <= no_price <= 1):
                    logger.warning(f"Invalid price range for {condition_id}: yes={yes_price}, no={no_price}")
                    return None
                return {"yes": yes_price, "no": no_price}

            # Try tokens array
            tokens = data.get("tokens", [])
            if len(tokens) >= 2:
                yes_price = float(tokens[0].get("price", 0))
                no_price = float(tokens[1].get("price", 0))
                if not (0 <= yes_price <= 1 and 0 <= no_price <= 1):
                    logger.warning(f"Invalid token price range for {condition_id}: yes={yes_price}, no={no_price}")
                    return None
                return {"yes": yes_price, "no": no_price}

            return None
        except (requests.RequestException, requests.HTTPError, ValueError, KeyError, TypeError, IndexError) as e:
            logger.debug(f"Failed to fetch CLOB price for {condition_id}: {e}")
            return None
