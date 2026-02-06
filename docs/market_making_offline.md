# Market Making Offline Mode

Offline mode lets you run the market making experiments without live Polymarket access by using recorded fixtures.

## Fixture Layout

```
fixtures/market_making/<profile>/
  markets.json
  snapshots/
    <market_id>.jsonl
```

### markets.json
- Array of market objects.
- Must include `conditionId` or `id`.
- Must be binary (`outcomes` with 2 entries, or `tokens` with 2 entries).

Example:
```json
[
  {
    "conditionId": "mkt1",
    "title": "Example market",
    "outcomes": ["YES", "NO"]
  }
]
```

### snapshots/<market_id>.jsonl
- JSONL file, one snapshot per line, ordered by time.
- Each snapshot should include:
  - `best_bid`, `best_ask`, `mid_price`
  - `depth_bid_1`, `depth_ask_1`
  - `last_trade_price`

Example line:
```json
{"best_bid":0.4975,"best_ask":0.5025,"mid_price":0.5,"depth_bid_1":1200,"depth_ask_1":1300,"last_trade_price":0.494}
```

## Offline Run Example

```bash
.venv/bin/python scripts/run_market_making_experiment.py \
  --data-mode offline \
  --fixture-dir fixtures/market_making/sample \
  --run-tag BASELINE \
  --bankroll 10000 \
  --markets 2 \
  --quote-size-usd 10 \
  --k-ticks 0 \
  --max-runtime-min 2 \
  --max-exposure-usd 5000 \
  --max-per-market-exposure-usd 500
```

Analyze + accept:
```bash
.venv/bin/python scripts/analyze_market_making.py \
  --db trades.db \
  --run-tag BASELINE \
  --run-id <RUN_ID> \
  --output-json reports/market_making_BASELINE.json \
  --output-md reports/market_making_BASELINE.md \
  --export-csv reports/market_making_BASELINE_fills.csv

.venv/bin/python scripts/check_market_making_run.py \
  --report-json reports/market_making_BASELINE.json \
  --fills-csv reports/market_making_BASELINE_fills.csv \
  --truthful-rate-min 0.99 \
  --max-exposure-usd 5000 \
  --max-per-market-exposure-usd 500 \
  --baseline
```
