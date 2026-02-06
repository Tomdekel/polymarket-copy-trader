import pytest

from market_making_strategy import MarketSnapshot, InventoryState, decide_quotes


def test_quote_generation_respects_ticks_and_never_crosses():
    snapshot = MarketSnapshot(
        market_id="mkt-1",
        best_bid=0.49,
        best_ask=0.51,
        mid_price=0.50,
        spread_pct=0.04,
        depth_bid_1=1000,
        depth_ask_1=1000,
        last_trade_price=0.50,
    )
    inventory = InventoryState(net_usd=0.0, gross_usd=0.0, oldest_hold_sec=None)
    decision = decide_quotes(
        snapshot,
        inventory,
        tick_size=0.01,
        k_ticks=1,
        max_spread_pct=0.10,
        max_per_market_exposure_usd=1000,
        max_total_exposure_usd=5000,
        skew_ticks=0.0,
    )
    assert decision.place_bid is True
    assert decision.place_ask is True
    assert decision.bid_price == pytest.approx(0.49, abs=1e-9)
    assert decision.ask_price == pytest.approx(0.51, abs=1e-9)
    assert decision.bid_price < decision.ask_price
