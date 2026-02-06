# Handoff

## Git Status
All market-making code was committed in `f680be7` on the `market-making` branch.
Prior to that commit, all MM files were untracked/unstaged on `master`.

## Status Summary
- Offline mode implemented via `data_provider.py` with `PolymarketAPIProvider` + `FixtureProvider`.
- Runner uses `--data-mode {online,offline}` + `--fixture-dir` + `--fixture-profile`.
- Sample fixtures added under `fixtures/market_making/sample/`.
- Baseline + real runs completed offline using sample fixtures.
- Analyzer + acceptance checker + dashboard working.
- **Scope note:** All experiments so far are offline-only. The online `PolymarketAPIProvider` exists in `data_provider.py` but has not been tested against live endpoints.

## Latest Run IDs (offline fixtures)
- Baseline: `mm-20260206T000710-86d86f`
- Real: `mm-20260206T001341-67ed37`

## Reports Generated
- `reports/market_making_BASELINE.json`
- `reports/market_making_BASELINE.md`
- `reports/market_making_BASELINE_fills.csv`
- `reports/market_making_MARKET_MAKING_TEST.json`
- `reports/market_making_MARKET_MAKING_TEST.md`
- `reports/market_making_MARKET_MAKING_TEST_fills.csv`
- `reports/mm_dashboard.html`

## Commands Used (offline)
Tests:
```bash
cd /Users/tomdekel/projects/polymarket-copy-trader
.venv/bin/python -m pytest tests/test_market_making_quotes.py \
  tests/test_market_making_inventory_caps.py \
  tests/test_market_making_trust_gate.py \
  tests/test_market_making_analyzer.py \
  tests/test_pnl.py \
  tests/test_measurement_mode.py \
  tests/test_execution_diagnostics.py
```

Baseline run:
```bash
.venv/bin/python scripts/run_market_making_experiment.py \
  --data-mode offline \
  --fixture-dir fixtures/market_making/sample \
  --run-tag BASELINE \
  --bankroll 10000 \
  --markets 2 \
  --quote-size-usd 10 \
  --k-ticks 0 \
  --max-runtime-min 1 \
  --max-exposure-usd 5000 \
  --max-per-market-exposure-usd 500 \
  --db reports/mm_baseline.db
```

Baseline analyze + accept:
```bash
.venv/bin/python scripts/analyze_market_making.py \
  --db reports/mm_baseline.db \
  --run-tag BASELINE \
  --run-id mm-20260206T000710-86d86f \
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

Real run (offline sample fixture):
```bash
.venv/bin/python scripts/run_market_making_experiment.py \
  --data-mode offline \
  --fixture-dir fixtures/market_making/sample \
  --run-tag MARKET_MAKING_TEST \
  --bankroll 10000 \
  --markets 2 \
  --quote-size-usd 10 \
  --k-ticks 2 \
  --max-runtime-min 1 \
  --max-exposure-usd 5000 \
  --max-per-market-exposure-usd 500 \
  --db reports/mm_real.db
```

Real analyze + accept:
```bash
.venv/bin/python scripts/analyze_market_making.py \
  --db reports/mm_real.db \
  --run-tag MARKET_MAKING_TEST \
  --run-id mm-20260206T001341-67ed37 \
  --output-json reports/market_making_MARKET_MAKING_TEST.json \
  --output-md reports/market_making_MARKET_MAKING_TEST.md \
  --export-csv reports/market_making_MARKET_MAKING_TEST_fills.csv

.venv/bin/python scripts/check_market_making_run.py \
  --report-json reports/market_making_MARKET_MAKING_TEST.json \
  --fills-csv reports/market_making_MARKET_MAKING_TEST_fills.csv \
  --truthful-rate-min 0.99 \
  --max-exposure-usd 5000 \
  --max-per-market-exposure-usd 500
```

Dashboard:
```bash
.venv/bin/python scripts/mm_dashboard.py \
  --reports reports/market_making_BASELINE.json reports/market_making_MARKET_MAKING_TEST.json \
  --output reports/mm_dashboard.html \
  --fills-dir reports
```

## Files Added
Core modules:
- `data_provider.py` — Online (`PolymarketAPIProvider`) and offline (`FixtureProvider`) data sources
- `market_making_engine.py` — Quoting engine with provider injection
- `market_making_strategy.py` — Strategy with k-tick and half-tick quoting
- `market_making_trust.py` — Trust gate for market validation
- `market_making_rewards.py` — Reward tracking for filled quotes
- `measurement_mode.py` — Measurement-only mode (no real execution)
- `execution_diagnostics.py` — Execution quality diagnostics
- `pnl.py` — P&L accounting (canonical source)
- `report_exports.py` — Report export utilities

Scripts:
- `scripts/run_market_making_experiment.py` — Experiment runner (offline/online)
- `scripts/analyze_market_making.py` — Post-run analyzer
- `scripts/check_market_making_run.py` — Acceptance checker (PASS/FAIL)
- `scripts/mm_dashboard.py` — HTML dashboard generator
- `scripts/record_polymarket_fixture.py` — Record live data as offline fixtures
- `scripts/offline_smoke_market_making.py` — Quick offline smoke test

Tests:
- `tests/test_market_making_quotes.py`
- `tests/test_market_making_inventory_caps.py`
- `tests/test_market_making_trust_gate.py`
- `tests/test_market_making_analyzer.py`
- `tests/test_pnl.py`
- `tests/test_measurement_mode.py`
- `tests/test_execution_diagnostics.py`

Fixtures/Config/Docs:
- `fixtures/market_making/sample/markets.json`
- `fixtures/market_making/sample/snapshots/sample-mkt-1.jsonl`
- `fixtures/market_making/sample/snapshots/sample-mkt-2.jsonl`
- `config/market_making_whitelist.json`
- `docs/market_making_offline.md`

## Files Modified (extended for MM)
- `config.yaml` — Added market-making config section
- `config_loader.py` — Load MM config keys
- `api_client.py` — Public Polymarket API endpoints (no auth)
- `database.py` — MM tables, queries, portfolio tracking
- `copy_trader.py` — Extended for MM alongside copy-trading
- `position_sizer.py` — MM position sizing support
- `risk_manager.py` — MM exposure limits
- `sheets_sync.py` — MM reporting columns
- `wallet_tracker.py` — MM wallet tracking
- `README.md` — Added MM section
- `tests/test_database.py` — MM database tests
- `tests/test_risk_manager.py` — MM risk tests

## Notes / Issues
- Offline sample fixture only has 2 markets. For the "real" 10-market run, record a larger fixture with `scripts/record_polymarket_fixture.py` and run with `--fixture-dir`.
- Online mode still works but requires network/DNS access to Polymarket.
- Analyzer still emits `datetime.utcnow()` deprecation warnings; not critical.
- `scripts/offline_smoke_market_making.py` is a utility smoke test but not wired into CI.

## Suggested Next Steps
1. Run online experiment on GCP VM (see Phase 3 of plan).
2. Record a larger fixture set (10+ Tier-A markets) using `scripts/record_polymarket_fixture.py`.
3. Run the longer 30-minute real offline experiment with that fixture.
4. Consider adding a small CI or local smoke target using `scripts/offline_smoke_market_making.py`.
