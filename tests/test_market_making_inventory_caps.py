from market_making_strategy import MarketSnapshot, InventoryState, decide_quotes


def test_inventory_caps_disable_increasing_exposure():
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
    inventory = InventoryState(net_usd=150.0, gross_usd=150.0, oldest_hold_sec=None)
    decision = decide_quotes(
        snapshot,
        inventory,
        tick_size=0.01,
        k_ticks=1,
        max_spread_pct=0.10,
        max_per_market_exposure_usd=100.0,
        max_total_exposure_usd=5000.0,
        skew_ticks=0.0,
    )
    assert decision.place_bid is False
    assert decision.place_ask is True
