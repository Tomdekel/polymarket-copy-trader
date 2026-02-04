"""Tests for wallet tracker module."""
import pytest
import responses
from responses import matchers

from wallet_tracker import WalletTracker, Position
from api_client import GAMMA_API_BASE
from utils import InvalidWalletAddressError
from tests.fixtures.gamma_api_responses import (
    SAMPLE_POSITIONS_RESPONSE,
    SAMPLE_BALANCE_RESPONSE,
    SAMPLE_MARKET_RESPONSE,
    SAMPLE_MARKETS_LIST_RESPONSE,
    EMPTY_POSITIONS_RESPONSE,
)


class TestWalletTrackerInit:
    """Test WalletTracker initialization."""

    def test_valid_wallet_address(self, valid_wallet_address):
        """Test initialization with valid wallet address."""
        tracker = WalletTracker(valid_wallet_address)
        assert tracker.wallet == valid_wallet_address.lower()

    def test_invalid_wallet_address(self):
        """Test initialization with invalid wallet addresses."""
        with pytest.raises(InvalidWalletAddressError):
            WalletTracker("")

        with pytest.raises(InvalidWalletAddressError):
            WalletTracker("not-a-wallet")

        with pytest.raises(InvalidWalletAddressError):
            WalletTracker("0x123")  # Too short


class TestWalletTrackerGetPositions:
    """Test WalletTracker.get_positions()."""

    @responses.activate
    def test_get_positions_success(self, valid_wallet_address):
        """Test successful position retrieval."""
        responses.add(
            responses.GET,
            f"{GAMMA_API_BASE}/portfolio/users/{valid_wallet_address.lower()}/positions",
            json=SAMPLE_POSITIONS_RESPONSE,
            status=200,
        )

        tracker = WalletTracker(valid_wallet_address)
        positions = tracker.get_positions()

        assert len(positions) == 2
        assert isinstance(positions[0], Position)
        assert positions[0].market == "0xabc123"
        assert positions[0].outcome == "YES"
        assert positions[0].size == 100.5
        assert positions[1].outcome == "NO"

    @responses.activate
    def test_get_positions_empty(self, valid_wallet_address):
        """Test empty positions response."""
        responses.add(
            responses.GET,
            f"{GAMMA_API_BASE}/portfolio/users/{valid_wallet_address.lower()}/positions",
            json=EMPTY_POSITIONS_RESPONSE,
            status=200,
        )

        tracker = WalletTracker(valid_wallet_address)
        positions = tracker.get_positions()

        assert positions == []

    @responses.activate
    def test_get_positions_api_error(self, valid_wallet_address):
        """Test API error handling."""
        responses.add(
            responses.GET,
            f"{GAMMA_API_BASE}/portfolio/users/{valid_wallet_address.lower()}/positions",
            json={"error": "Not found"},
            status=404,
        )

        tracker = WalletTracker(valid_wallet_address)
        with pytest.raises(RuntimeError, match="Failed to fetch positions"):
            tracker.get_positions()


class TestWalletTrackerGetPortfolioValue:
    """Test WalletTracker.get_portfolio_value()."""

    @responses.activate
    def test_get_portfolio_value_success(self, valid_wallet_address):
        """Test successful portfolio value retrieval."""
        responses.add(
            responses.GET,
            f"{GAMMA_API_BASE}/portfolio/users/{valid_wallet_address.lower()}/balance",
            json=SAMPLE_BALANCE_RESPONSE,
            status=200,
        )

        tracker = WalletTracker(valid_wallet_address)
        value = tracker.get_portfolio_value()

        assert value == 87.85

    @responses.activate
    def test_get_portfolio_value_api_error(self, valid_wallet_address):
        """Test API error handling."""
        responses.add(
            responses.GET,
            f"{GAMMA_API_BASE}/portfolio/users/{valid_wallet_address.lower()}/balance",
            status=500,
        )

        tracker = WalletTracker(valid_wallet_address)
        with pytest.raises(RuntimeError, match="Failed to fetch portfolio value"):
            tracker.get_portfolio_value()


class TestWalletTrackerGetMarketPrice:
    """Test WalletTracker.get_market_price()."""

    @responses.activate
    def test_get_market_price_yes(self, valid_wallet_address):
        """Test getting YES outcome price."""
        responses.add(
            responses.GET,
            f"{GAMMA_API_BASE}/markets/0xabc123",
            json=SAMPLE_MARKET_RESPONSE,
            status=200,
        )

        tracker = WalletTracker(valid_wallet_address)
        price = tracker.get_market_price("0xabc123", "YES")

        assert price == 0.70

    @responses.activate
    def test_get_market_price_no(self, valid_wallet_address):
        """Test getting NO outcome price."""
        responses.add(
            responses.GET,
            f"{GAMMA_API_BASE}/markets/0xabc123",
            json=SAMPLE_MARKET_RESPONSE,
            status=200,
        )

        tracker = WalletTracker(valid_wallet_address)
        price = tracker.get_market_price("0xabc123", "NO")

        assert price == 0.30

    @responses.activate
    def test_get_market_price_api_error_returns_none(self, valid_wallet_address):
        """Test that API errors return None instead of raising."""
        responses.add(
            responses.GET,
            f"{GAMMA_API_BASE}/markets/0xabc123",
            status=404,
        )

        tracker = WalletTracker(valid_wallet_address)
        price = tracker.get_market_price("0xabc123")

        assert price is None


class TestWalletTrackerGetMarkets:
    """Test WalletTracker.get_markets()."""

    @responses.activate
    def test_get_markets_success(self, valid_wallet_address):
        """Test successful markets retrieval."""
        responses.add(
            responses.GET,
            f"{GAMMA_API_BASE}/markets",
            json=SAMPLE_MARKETS_LIST_RESPONSE,
            status=200,
        )

        tracker = WalletTracker(valid_wallet_address)
        markets = tracker.get_markets()

        assert len(markets) == 2
        assert markets[0]["slug"] == "will-bitcoin-reach-100k"
