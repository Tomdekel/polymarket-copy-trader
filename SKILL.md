# SKILL.md — Execution Diagnostics, Measurement Mode, and Slippage Analysis

This repository implements a copy-trading system with **strict accounting guarantees** and **first-class execution diagnostics**.
Claude/Codex should treat this as an auditable system where **measurement precedes optimization**.

This document describes the system's capabilities and how to use them correctly.

---

## Core Principles (DO NOT VIOLATE)

1. **Accounting correctness > strategy performance**
   - All P&L must be computed via `pnl.py`.
   - Shares ≠ USD size. This invariant is enforced and must never be bypassed.
   - Portfolio identity must hold:
     ```
     portfolio_current_value = cash + sum(open_position_values)
     ```

2. **Fail fast on broken invariants**
   - In LIVE mode, reconciliation failures halt trading.
   - In BACKTEST / DRY_RUN, failures raise assertions.
   - Never suppress these errors.

3. **Measure before optimizing**
   - Execution quality must be diagnosed before changing strategy.
   - Guardrails (tier gating, hysteresis) must not bias measurement runs.

---

## System Capabilities

### 1. Accounting & Reconciliation
- Authoritative P&L logic in `pnl.py`
- Strict field semantics:
  - shares
  - entry_price
  - exit_price
  - cost_basis_usd
  - proceeds_usd
- Last-mile reconciliation gate ensures ledger, portfolio, and reports match.
- Reporting parity across DB → Sheets → PDF is enforced by integration tests.

**Claude/Codex must not re-implement P&L or recompute it in exports.**

---

### 2. Execution Diagnostics (Slippage Instrumentation)

The system records **structured execution diagnostics** for every order and fill:

Captured dimensions:
- Whale reference (real or synthetic)
- Decision, send, ack, fill timestamps
- Bid / ask / mid / spread
- Top-of-book depth
- Fill price and size

Derived metrics:
- latency_ms
- quote_slippage_pct
- baseline_slippage_pct
- spread_crossed
- impact proxy

Data is stored durably in SQLite and exportable to CSV.

---

### 3. Measurement Mode (Controlled Experiments)

Measurement mode exists to **generate unbiased execution data**.

Key features:
- Explicit flag: `--measurement-mode`
- Tiny fixed position sizes
- Hard exposure caps
- Per-order timeout
- Snapshot validation
- Abort-on-error with debug dump

Synthetic baseline in measurement mode:
- `whale_ref_type = "synthetic"`
- `whale_entry_ref_price = mid_price at decision time`
- `whale_signal_ts = decision_ts`

This isolates **pure execution slippage** from whale timing effects.

---

### 4. Deterministic Market Sampling

Markets are classified into liquidity tiers:
- **Tier A**: tight spread, deep book
- **Tier B**: medium liquidity
- **Tier C**: thin markets

Tiering is deterministic and snapshot-based, enabling reproducible experiments.

---

### 5. End-to-End Slippage Experiments

A single command can:
1. Run a measurement batch
2. Export diagnostics
3. Analyze results
4. Print key summaries

Example:
```bash
.venv/bin/python scripts/run_slippage_experiment.py \
  --db trades.db \
  --n 30 \
  --max-size-usd 5
```

---

## Testing Requirements

- 201 tests across 11 modules
- `test_pnl.py` covers low-price markets, edge cases, resolution scenarios
- Run `pytest tests/` before any accounting-related changes
- Never mark a PR ready if tests fail

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `pnl.py` | Authoritative P&L calculations |
| `database.py` | Trade ledger, reconciliation gates |
| `risk_manager.py` | Loss recording, trading halts |
| `execution_diagnostics.py` | Slippage instrumentation |
| `measurement_mode.py` | Controlled experiments |
| `sheets_sync.py` | Google Sheets export (uses pnl.py) |
