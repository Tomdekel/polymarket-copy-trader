# Market Making GCP Experiment Handoff

## A) What Exists in the Repo

The `market-making` branch contains a complete offline/online market-making experiment framework:

**Core modules:** `data_provider.py` (online `PolymarketAPIProvider` + offline `FixtureProvider`), `market_making_engine.py` (quoting engine), `market_making_strategy.py` (k-tick quoting), `market_making_trust.py` (trust gates), `market_making_rewards.py`, `measurement_mode.py`, `execution_diagnostics.py`, `pnl.py`, `report_exports.py`.

**Scripts:**
- `scripts/run_market_making_experiment.py` — Main runner (offline/online), prints `Run complete: run_id=...`
- `scripts/analyze_market_making.py` — Post-run analyzer, writes JSON + MD + CSV
- `scripts/check_market_making_run.py` — Acceptance checker, prints PASS/FAIL
- `scripts/mm_dashboard.py` — HTML dashboard comparing runs
- `scripts/record_polymarket_fixture.py` — Record live data as offline fixtures
- `scripts/offline_smoke_market_making.py` — Quick offline smoke test

**Offline fixtures:** `fixtures/market_making/sample/` (2 markets, 3 snapshots each).

**Whitelist:** `config/market_making_whitelist.json` — curated condition IDs for online runs.

**Acceptance criteria:** Truthful rate >= 99%, total exposure <= cap, per-market exposure <= cap, reconciliation pass. 0-fill runs pass automatically (no violations possible).

## B) GCP Runtime Choice: Compute Engine VM (e2-small)

**Why VM over Cloud Run Job:**
- No Dockerfile changes needed (current Dockerfile only copies `*.py` + `config.yaml`, missing `scripts/`, `fixtures/`, `config/`)
- Direct SSH for troubleshooting
- Sequential command execution (baseline -> check -> real -> check -> dashboard)
- Simple provisioning and teardown
- 30-min experiment needs stable session (`nohup` for safety)

**Estimated cost:** e2-small ($0.0168/hr) x 1 hr = ~$0.02.

## C) Step-by-Step Reproduction

### 1. Create GCS bucket (one-time)
```bash
gcloud storage buckets create gs://tombot-485015-mm-experiments \
  --location=us-central1 --uniform-bucket-level-access
```

### 2. Provision VM
```bash
gcloud compute instances create mm-experiment-vm \
  --zone=us-central1-a \
  --machine-type=e2-small \
  --image-family=debian-12 \
  --image-project=debian-cloud \
  --boot-disk-size=20GB \
  --scopes=storage-rw,compute-ro
```

### 3. Setup on VM
```bash
gcloud compute ssh mm-experiment-vm --zone=us-central1-a --command='
  sudo apt-get update -qq &&
  sudo apt-get install -y -qq python3-pip python3-venv git > /dev/null 2>&1 &&
  git clone --branch market-making https://github.com/Tomdekel/polymarket-copy-trader.git &&
  cd polymarket-copy-trader &&
  python3 -m venv .venv &&
  .venv/bin/pip install -q -r requirements.txt
'
```

### 4. Connectivity check
```bash
gcloud compute ssh mm-experiment-vm --zone=us-central1-a --command='
  cd polymarket-copy-trader &&
  .venv/bin/python3 -c "
import socket
addrs = socket.getaddrinfo(\"gamma-api.polymarket.com\", 443)
print(f\"DNS OK: {len(addrs)} addresses\")
from api_client import GammaAPIClient
c = GammaAPIClient()
markets = c.get_markets(active=True, closed=False)
print(f\"Markets: {len(markets)}\")
print(\"CONNECTIVITY_OK\")
"'
```

### 5. Baseline run (10 min, k=0)
```bash
gcloud compute ssh mm-experiment-vm --zone=us-central1-a --command='
  cd polymarket-copy-trader &&
  .venv/bin/python scripts/run_market_making_experiment.py \
    --data-mode online --db trades.db --run-tag BASELINE \
    --bankroll 10000 --markets 5 --quote-size-usd 10 --k-ticks 0 \
    --max-runtime-min 10 --max-exposure-usd 5000 --max-per-market-exposure-usd 500
'
# Capture run_id from output: Run complete: run_id=mm-YYYYMMDDTHHMMSS-XXXXXX
```

### 6. Baseline analysis + acceptance
```bash
gcloud compute ssh mm-experiment-vm --zone=us-central1-a --command='
  cd polymarket-copy-trader &&
  .venv/bin/python scripts/analyze_market_making.py \
    --db trades.db --run-tag BASELINE --run-id <BASELINE_RUN_ID> \
    --output-json reports/market_making_BASELINE.json \
    --output-md reports/market_making_BASELINE.md \
    --export-csv reports/market_making_BASELINE_fills.csv &&
  .venv/bin/python scripts/check_market_making_run.py \
    --report-json reports/market_making_BASELINE.json \
    --fills-csv reports/market_making_BASELINE_fills.csv \
    --truthful-rate-min 0.99 --max-exposure-usd 5000 \
    --max-per-market-exposure-usd 500 --baseline
'
```

### 7. Real run (30 min, k=2) — use nohup
```bash
gcloud compute ssh mm-experiment-vm --zone=us-central1-a --command='
  cd polymarket-copy-trader &&
  nohup .venv/bin/python scripts/run_market_making_experiment.py \
    --data-mode online --db trades.db --run-tag MARKET_MAKING_TEST \
    --bankroll 10000 --markets 10 --quote-size-usd 10 --k-ticks 2 \
    --max-runtime-min 30 --max-exposure-usd 5000 --max-per-market-exposure-usd 500 \
    > /tmp/mm_real_run.log 2>&1 &
  echo "Started PID=$!"
'
# Wait ~32 min, then check:
gcloud compute ssh mm-experiment-vm --zone=us-central1-a --command='cat /tmp/mm_real_run.log'
```

### 8. Real analysis + acceptance
```bash
# Same as step 6 but with MARKET_MAKING_TEST run_id and without --baseline flag
```

### 9. Dashboard
```bash
gcloud compute ssh mm-experiment-vm --zone=us-central1-a --command='
  cd polymarket-copy-trader &&
  .venv/bin/python scripts/mm_dashboard.py \
    --reports reports/market_making_BASELINE.json reports/market_making_MARKET_MAKING_TEST.json \
    --output reports/mm_dashboard.html --fills-dir reports
'
```

### 10. Upload to GCS
```bash
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
gcloud compute ssh mm-experiment-vm --zone=us-central1-a --command="
  cd polymarket-copy-trader &&
  gsutil -m cp reports/*.json reports/*.md reports/*.csv reports/*.html trades.db \
    gs://tombot-485015-mm-experiments/online-run-${TIMESTAMP}/
"
```

### 11. Delete VM
```bash
gcloud compute instances delete mm-experiment-vm --zone=us-central1-a --quiet
```

## D) GCS Artifact Layout

```
gs://tombot-485015-mm-experiments/
  online-run-20260206/
    market_making_BASELINE.json          # Analyzer JSON output
    market_making_BASELINE.md            # Human-readable report
    market_making_BASELINE_fills.csv     # Fill-level data
    market_making_MARKET_MAKING_TEST.json
    market_making_MARKET_MAKING_TEST.md
    market_making_MARKET_MAKING_TEST_fills.csv
    mm_dashboard.html                    # Side-by-side comparison
    trades.db                            # SQLite with all trades/positions
    run_log.txt                          # Commands, run_ids, timestamps
```

## E) Failure Modes + Fixes

| Failure | Symptom | Fix |
|---------|---------|-----|
| DNS resolution fails | `socket.gaierror` | Check VM firewall; default VPC allows egress. Try `nslookup gamma-api.polymarket.com` |
| Gamma API 404 | `/markets` returns 404 | API may have moved again. Check `https://gamma-api.polymarket.com/markets?limit=1` manually |
| CLOB book empty | All snapshots have `best_bid=None` | Normal for thin markets. The Gamma fallback (`outcomePrices`) provides mid. Check `config/market_making_whitelist.json` has valid CIDs |
| 429 rate limit | `HTTPError: 429` | Engine skips that market on that tick. If widespread, reduce `--markets` count or add `time.sleep()` between iterations |
| No Tier-A markets found | `SystemExit: No markets selected` | Use `--whitelist config/market_making_whitelist.json` with curated IDs |
| Missing deps on VM | `ImportError` | Re-run `pip install -r requirements.txt` in venv |
| Clock skew | Trust gate `mid_mismatch` failures | Run `sudo ntpdate ntp.ubuntu.com` or check TZ handling |
| SSH drops during 30-min run | Process killed | Use `nohup` (as shown above). Check with `ps aux | grep run_market_making` |
| Stale whitelist | Markets in whitelist are now closed | Re-run the whitelist builder script (see scan_markets section) or manually update `config/market_making_whitelist.json` |

## F) Stop Conditions and What to Check First

**When the acceptance checker says FAIL:**
1. Read the FAIL message — it tells you exactly which check failed
2. Look at the analyzer JSON for details (`truth`, `pnl`, `inventory` sections)
3. If `truthful_rate` is low: check fills CSV for suspicious fill prices
4. If exposure exceeded: check `max_inventory_held_by_market` in JSON

**When 0 fills occur (expected in measurement mode):**
- This is normal for online mode. Polymarket CLOB books have extreme resting orders (0.01/0.99) while actual trading happens near mid.
- Our simulated fills require `last_trade_price` to match our limit quotes, which rarely happens in 30 min.
- The value is proving the engine runs cleanly with live data, not generating fills.

**When to stop iterating:**
- Engine crashes with unhandled exception → fix the bug
- Exposure limits violated → check quoting logic
- Reconciliation fails → check P&L accounting in `pnl.py`

## G) Probabilistic Fill Model (Implemented)

The pluggable fill model system was added in commit `4e87ed0`. Two models:

- **`strict`** (default): Original deterministic crossing — fill iff `last_trade_price` crosses quote. Produces 0 fills online because CLOB book structure doesn't match.
- **`probabilistic`**: `p = min(p_max, base_liquidity * exp(-alpha * dist_ticks))` where `dist_ticks = abs(quote - ref) / tick_size`. Deterministic given `--seed`.

### CLI flags
```
--fill-model probabilistic  # or strict (default)
--seed 42                   # RNG seed for reproducibility
--fill-alpha 1.5            # exponential decay rate
--fill-pmax 0.20            # max per-step fill probability
--fill-base-liquidity 0.10  # base liquidity parameter
```

### Calibration
```bash
python scripts/calibrate_fill_model.py --fixture-dir fixtures/market_making/sample --k-ticks 2
```

### Online results (2026-02-06)
| Run | Fill Model | k-ticks | Runtime | Markets | Fills | Acceptance |
|-----|-----------|---------|---------|---------|-------|------------|
| `mm-20260206T020928-c0693f` | strict | 0 | 10 min | 5 | 0 | PASS |
| `mm-20260206T021950-c80184` | probabilistic | 2 | 30 min | 10 | 24 | PASS |

Artifacts: `gs://tombot-485015-mm-experiments/online-run-prob-fill-20260206T025253Z/`

## H) Next Safe Iterations

1. **Record larger fixtures:** Use `scripts/record_polymarket_fixture.py` to capture 10+ markets x 100+ snapshots from live API, then run offline with realistic data.

2. **Tune fill model parameters:** Use `scripts/calibrate_fill_model.py` against larger fixtures. Current defaults (alpha=1.5, base_liq=0.10, p_max=0.20) produce ~4% fill rate offline.

3. **Longer online runs:** Increase `--max-runtime-min` to 120+ on a larger VM to accumulate more fills and test P&L accounting under load.

4. **Add intermediate logging:** Modify the runner to log quote decisions and snapshot data every N iterations for debugging.

5. **Dockerize properly:** Update `Dockerfile` to include `scripts/`, `fixtures/`, `config/` so Cloud Run Jobs become viable for scheduled runs.

6. **Real order submission:** Once fills are validated in simulation, integrate with Polymarket's actual order API (requires authentication and wallet setup).
