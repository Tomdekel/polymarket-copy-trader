"""Mock responses for Gamma API."""

SAMPLE_POSITIONS_RESPONSE = {
    "positions": [
        {
            "market": "0xabc123",
            "market_slug": "will-bitcoin-reach-100k",
            "outcome_index": 0,
            "size": "100.5",
            "avg_price": "0.65",
            "current_price": "0.70",
            "value": "70.35",
            "pnl": "5.025",
        },
        {
            "market": "0xdef456",
            "market_slug": "us-election-2024",
            "outcome_index": 1,
            "size": "50.0",
            "avg_price": "0.40",
            "current_price": "0.35",
            "value": "17.50",
            "pnl": "-2.50",
        },
    ]
}

SAMPLE_BALANCE_RESPONSE = {
    "total_value": 87.85,
    "cash_balance": 0,
    "positions_value": 87.85,
}

SAMPLE_MARKET_RESPONSE = {
    "id": "0xabc123",
    "slug": "will-bitcoin-reach-100k",
    "question": "Will Bitcoin reach $100k by end of 2024?",
    "outcomePrices": ["0.70", "0.30"],
    "yes_price": "0.70",
    "no_price": "0.30",
    "bestBid": "0.69",
    "bestAsk": "0.71",
    "volume": "1234567.89",
    "liquidity": "50000.00",
}

SAMPLE_MARKETS_LIST_RESPONSE = {
    "markets": [
        {
            "id": "0xabc123",
            "slug": "will-bitcoin-reach-100k",
            "question": "Will Bitcoin reach $100k?",
            "active": True,
            "closed": False,
        },
        {
            "id": "0xdef456",
            "slug": "us-election-2024",
            "question": "US Election 2024",
            "active": True,
            "closed": False,
        },
    ]
}

EMPTY_POSITIONS_RESPONSE = {"positions": []}

ERROR_RESPONSE = {"error": "Not found", "status": 404}
