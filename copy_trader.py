#!/usr/bin/env python3
"""Polymarket Copy Trader - Main CLI entry point."""
import click
import time
import sys
import signal
import threading
import uuid
from pathlib import Path
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Dict, Any, Optional

from utils import setup_logging, console, format_currency, truncate_address, validate_wallet_address, InvalidWalletAddressError
from wallet_tracker import WalletTracker
from database import Database
from position_sizer import PositionSizer
from risk_manager import RiskManager
from config_loader import load_config, get_db_path, get_webhook_url
from sheets_sync import create_sheets_sync
from health_server import HealthServer, get_health_status
from notifications import NotificationService
from execution_diagnostics import ExecutionDiagnostics
from measurement_mode import MeasurementSelector, build_synthetic_baseline

# Global shutdown event for graceful termination
shutdown_event = threading.Event()


def signal_handler(signum: int, frame) -> None:
    """Handle shutdown signals gracefully."""
    signal_name = signal.Signals(signum).name
    console.print(f"\n[bold yellow]Received {signal_name}, shutting down...[/bold yellow]")
    shutdown_event.set()


# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--config', '-c', type=click.Path(), default=None,
              help='Path to config file (optional, uses config.yaml by default)')
@click.pass_context
def cli(ctx, config):
    """Polymarket Copy Trader - Copy trades from any wallet."""
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = config

    # Load config with environment variable support
    ctx.obj['config'] = load_config(config)

    # Setup logging
    log_level = ctx.obj['config'].get('reporting', {}).get('log_level', 'INFO')
    ctx.obj['logger'] = setup_logging(log_level)


def validate_budget(ctx, param, value):
    """Validate budget is positive."""
    if value is not None and value <= 0:
        raise click.BadParameter("Budget must be a positive number")
    return value


@cli.command()
@click.option('--wallet', '-w', required=True, help='Target wallet address to copy')
@click.option('--budget', '-b', type=float, required=True, callback=validate_budget,
              help='Your trading budget in USD (must be positive)')
@click.option('--dry-run/--live', default=True, help='Run in simulation mode (default) or live')
@click.option('--health-port', type=int, default=8080, help='Port for health check server')
@click.option('--measurement-mode/--no-measurement-mode', default=False, help='Run controlled measurement fills only')
@click.option('--measurement-trades', type=int, default=30, help='Number of completed measurement fills before auto-stop')
@click.option('--measurement-max-size-usd', type=float, default=5.0, help='Max USD size per measurement trade')
@click.option('--measurement-market-filter', type=click.Choice(['all', 'A', 'B', 'C'], case_sensitive=False), default='all', help='Optional liquidity tier filter for measurement mode')
@click.option('--run-tag', type=str, default=None, help='Optional cohort tag for execution diagnostics')
@click.pass_context
def copy(
    ctx,
    wallet: str,
    budget: float,
    dry_run: bool,
    health_port: int,
    measurement_mode: bool,
    measurement_trades: int,
    measurement_max_size_usd: float,
    measurement_market_filter: str,
    run_tag: Optional[str],
):
    """Start copying trades from a target wallet."""
    config = ctx.obj['config']
    logger = ctx.obj['logger']

    # Validate wallet address
    try:
        wallet = validate_wallet_address(wallet)
    except InvalidWalletAddressError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)

    # Override config with CLI args
    config['target_wallet'] = wallet
    config['starting_budget'] = budget
    config['execution']['dry_run'] = dry_run
    config['execution']['measurement_mode'] = measurement_mode
    config['execution']['measurement_trades'] = measurement_trades
    config['execution']['measurement_max_size_usd'] = measurement_max_size_usd
    config['execution']['measurement_market_filter'] = measurement_market_filter
    if measurement_mode:
        if measurement_trades <= 0:
            raise click.BadParameter("--measurement-trades must be > 0")
        if measurement_max_size_usd <= 0:
            raise click.BadParameter("--measurement-max-size-usd must be > 0")
    enable_execution_guardrails = bool(config.get("execution", {}).get("enable_execution_guardrails", False))
    if measurement_mode and not enable_execution_guardrails:
        config.setdefault("execution", {})["enable_execution_guardrails"] = False
    run_tag = run_tag or config.get("execution", {}).get("run_tag") or ("measurement" if measurement_mode else "default")
    config.setdefault("execution", {})["run_tag"] = run_tag

    # Start health server for Cloud Run
    health_server = HealthServer(port=health_port)
    health_server.start()
    health_status = get_health_status()

    # Initialize notification service
    webhook_url = get_webhook_url(config)
    notifier = NotificationService(webhook_url)

    # Initialize Google Sheets sync
    sheets_sync = create_sheets_sync(config)
    if sheets_sync:
        console.print(f"Sheets Dashboard: [green]Enabled[/green]")
    else:
        console.print(f"Sheets Dashboard: [dim]Disabled[/dim]")

    # Initialize components
    db_path = get_db_path(config)
    db = Database(db_path)
    db.initialize_portfolio(budget)
    integrity_issues = db.validate_trade_integrity()
    if integrity_issues:
        for issue in integrity_issues[:20]:
            logger.error("Data integrity issue: %s", issue)
        if len(integrity_issues) > 20:
            logger.error("...and %d more integrity issues", len(integrity_issues) - 20)
        if not dry_run:
            raise RuntimeError("Trade integrity check failed in live mode; refusing to continue")
    tracker = WalletTracker(wallet)
    sizer = PositionSizer(budget, config)
    risk_mgr = RiskManager(config)
    risk_mgr.set_starting_budget(budget)
    recon_eps = float(config.get("risk_management", {}).get("reconciliation_epsilon", 1e-6))
    mode = "backtest" if dry_run else "live"
    quality_cfg = config.get("execution_quality", {})
    liquidity_low_threshold = float(quality_cfg.get("low_tier_min_liquidity", 5000))
    low_tier_stability_seconds = int(quality_cfg.get("low_tier_stability_seconds", 300))
    rebalance_cooldown_seconds = int(quality_cfg.get("rebalance_cooldown_seconds", 180))
    drift_threshold_high = float(quality_cfg.get("drift_threshold_high", 0.30))
    order_type = config.get("execution", {}).get("order_type", "limit")
    time_in_force = config.get("execution", {}).get("time_in_force")
    run_id = config.get("execution", {}).get("run_id") or f"run-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
    diagnostics = ExecutionDiagnostics(
        db_path=db_path,
        csv_path=config.get("reporting", {}).get("slippage_csv")
        or config.get("reporting", {}).get("execution_diagnostics_csv")
        or "slippage.csv",
        live_mode=not dry_run,
    )
    market_stability: Dict[str, Dict[str, Any]] = {}
    market_last_rebalance_ts: Dict[str, float] = {}
    measurement_selector = MeasurementSelector()
    measurement_plan = []
    measurement_tier_by_market: Dict[str, str] = {}
    measurement_index = 0
    measurement_fills = 0
    measurement_order_timeout_s = int(config.get("execution", {}).get("measurement_order_timeout_s", 15))
    measurement_market_filter = (measurement_market_filter or "all").upper()
    measurement_total_exposure_cap = min(float(budget), float(measurement_max_size_usd) * 2.0)

    # Display startup info
    console.print("\n[bold blue]Polymarket Copy Trader[/bold blue]")
    console.print(f"Target Wallet: [cyan]{truncate_address(wallet)}[/cyan]")
    console.print(f"Your Budget: [green]{format_currency(budget)}[/green]")
    console.print(f"Mode: [yellow]{'DRY RUN (simulation)' if dry_run else 'LIVE'}[/yellow]")
    if measurement_mode:
        console.print(
            f"Measurement: [yellow]enabled[/yellow] "
            f"(fills={measurement_trades}, max_size_usd={measurement_max_size_usd:.2f}, tier={measurement_market_filter})"
        )
    console.print(f"Health endpoint: [cyan]http://localhost:{health_port}/health[/cyan]")
    if webhook_url:
        console.print(f"Notifications: [green]Enabled[/green]\n")
    else:
        console.print(f"Notifications: [dim]Disabled[/dim]\n")

    # Send startup notification
    notifier.send_startup_notification(wallet, budget, dry_run)

    check_interval = config['execution'].get('check_interval', 30)
    trades_count = 0

    # Clear any previous shutdown state
    shutdown_event.clear()

    try:
        while not shutdown_event.is_set():
            # Get target wallet positions
            try:
                target_positions = tracker.get_positions()
                target_value = tracker.get_portfolio_value()
            except RuntimeError as e:
                logger.error(f"Failed to fetch target wallet: {e}")
                shutdown_event.wait(check_interval)
                continue

            # Create price lookup from target positions (avoids failing API call)
            # Also store outcome and slug for fallback price fetching and display
            price_map = {pos.market: pos.current_price for pos in target_positions}
            outcome_map = {pos.market: pos.outcome for pos in target_positions}
            slug_map = {pos.market: pos.market_slug for pos in target_positions}
            target_pos_map = {p.market: p for p in target_positions}

            # Get our current positions and portfolio stats
            our_positions = db.get_open_positions()

            # Update P&L for all our open positions using current prices
            for position in our_positions:
                market_id = position.get('market')
                current_price = price_map.get(market_id)
                if current_price is not None:  # Use 'is not None' to handle 0.0 price
                    db.update_trade_pnl(position['id'], current_price, current_price_source="whale_ref")

            # Re-fetch positions to get updated P&L values
            our_positions = db.get_open_positions()
            db.run_reconciliation_gate(mode=mode, eps=recon_eps)

            # Reconcile from ledger so P&L math is internally consistent.
            reconciliation = db.reconcile_portfolio(starting_equity=budget)
            unrealized_pnl = reconciliation.get("total_unrealized", 0.0)
            realized_pnl = reconciliation.get("total_realized", 0.0)
            total_pnl_for_risk = unrealized_pnl + realized_pnl
            daily_realized_pnl = db.calculate_24h_pnl()
            weekly_realized_pnl = db.calculate_7d_pnl()
            risk_check = risk_mgr.check_risk(
                current_pnl=total_pnl_for_risk,
                daily_pnl=daily_realized_pnl + unrealized_pnl,
                weekly_pnl=weekly_realized_pnl + unrealized_pnl,
            )

            if not risk_check['allow_trade']:
                logger.warning(f"Risk halt: {risk_check['reason']}")
                notifier.send_risk_alert(
                    reason=risk_check['reason'],
                    current_pnl=total_pnl_for_risk,
                    daily_pnl=db.calculate_24h_pnl(),
                )
                shutdown_event.wait(check_interval)
                continue

            # Calculate position sizes
            if measurement_mode:
                if measurement_fills >= measurement_trades:
                    logger.info("Measurement target reached (%d fills). Stopping.", measurement_fills)
                    break
                if not measurement_plan:
                    snapshots = {}
                    for pos in target_positions:
                        snapshots[pos.market] = tracker.get_market_snapshot(pos.market, pos.outcome)
                    candidates = measurement_selector.build_candidates(target_positions, snapshots)
                    measurement_plan = measurement_selector.select_cycle(
                        candidates,
                        n=max(measurement_trades, 1),
                        market_filter=None if measurement_market_filter == "ALL" else measurement_market_filter,
                    )
                    measurement_tier_by_market = {c.market_id: c.tier for c in candidates}
                    measurement_index = 0
                    if not measurement_plan:
                        raise RuntimeError("Measurement mode could not select any markets")
                market_pick = measurement_plan[measurement_index % len(measurement_plan)]
                measurement_index += 1
                sized_positions = [
                    SimpleNamespace(
                        action="BUY",
                        market=market_pick.market_id,
                        target_percentage=0.0,
                        our_size=min(measurement_max_size_usd, budget),
                        drift_pct=1.0,
                    ),
                    SimpleNamespace(
                        action="SELL",
                        market=market_pick.market_id,
                        target_percentage=0.0,
                        our_size=min(measurement_max_size_usd, budget),
                        drift_pct=1.0,
                    ),
                ]
            else:
                sized_positions = sizer.calculate_positions(
                    target_value,
                    [{'market': p.market, 'size': p.size, 'value': p.value} for p in target_positions],
                    our_positions
                )
            loop_time = time.time()

            # Execute trades
            for pos in sized_positions:
                if shutdown_event.is_set():
                    break

                if pos.action == "HOLD":
                    continue
                target_pos = target_pos_map.get(pos.market)
                if config.get("execution", {}).get("enable_execution_guardrails", False):
                    raw_liquidity = getattr(target_pos, "liquidity", None)
                    market_liquidity = float(raw_liquidity) if raw_liquidity is not None else liquidity_low_threshold
                    whale_position_size = float(getattr(target_pos, "size", 0) or 0)

                    # Liquidity tiering: low-liquidity markets require stability window.
                    if market_liquidity < liquidity_low_threshold:
                        state = market_stability.get(pos.market)
                        if not state or abs(state["target_size"] - whale_position_size) > 1e-9:
                            market_stability[pos.market] = {"target_size": whale_position_size, "first_seen": loop_time}
                            logger.info(
                                "Skip %s (low liquidity tier %.2f < %.2f, waiting for stability window)",
                                pos.market,
                                market_liquidity,
                                liquidity_low_threshold,
                            )
                            continue
                        stable_seconds = loop_time - state["first_seen"]
                        if stable_seconds < low_tier_stability_seconds:
                            logger.info(
                                "Skip %s (low liquidity stable for %ss, need %ss)",
                                pos.market,
                                int(stable_seconds),
                                low_tier_stability_seconds,
                            )
                            continue

                    # Hysteresis: prevent rapid repeated rebalances unless drift is very high.
                    last_rebalance = market_last_rebalance_ts.get(pos.market)
                    if (
                        last_rebalance is not None
                        and (loop_time - last_rebalance) < rebalance_cooldown_seconds
                        and pos.drift_pct < drift_threshold_high
                    ):
                        logger.info(
                            "Skip %s due to hysteresis (cooldown active, drift=%.2f%% < %.2f%%)",
                            pos.market,
                            pos.drift_pct * 100,
                            drift_threshold_high * 100,
                        )
                        continue

                msg = f"{pos.action}: {pos.market} | Target: {pos.target_percentage:.1%} | Our size: {format_currency(pos.our_size)}"

                if dry_run:
                    logger.info(f"[DRY RUN] {msg}")
                else:
                    logger.info(msg)
                    # TODO: Execute actual trade via executor

                # Record trade with current market price
                if pos.action != "HOLD":
                    action_side = pos.action.lower()
                    outcome = outcome_map.get(pos.market, "YES")
                    decision_ts = datetime.now(UTC)
                    snapshot: Dict[str, Optional[float]]
                    try:
                        snapshot = tracker.get_market_snapshot(pos.market, outcome)
                    except Exception as e:
                        logger.warning(f"Failed to fetch snapshot for {pos.market}: {e}")
                        snapshot = {
                            "best_bid": None,
                            "best_ask": None,
                            "mid_price": None,
                            "depth_bid_1": None,
                            "depth_ask_1": None,
                            "last_trade_price": None,
                        }

                    # Get price from target positions (already fetched from API)
                    current_price = price_map.get(pos.market)

                    # If price missing, try to fetch via CLOB API
                    if current_price is None:
                        try:
                            fetched_price = tracker.get_market_price(pos.market, outcome)
                            if fetched_price is not None:
                                current_price = fetched_price
                                logger.info(f"Fetched current price {current_price:.4f} for {pos.market}")
                        except Exception as e:
                            logger.warning(f"Failed to fetch price for {pos.market}: {e}")

                    if current_price is None:
                        logger.warning(f"No price data for {pos.market}, using fallback 0.5")
                        current_price = 0.5
                        current_price_source = "placeholder"
                    elif pos.market in price_map and price_map.get(pos.market) is not None:
                        current_price_source = "whale_ref"
                    else:
                        current_price_source = "quote"
                    current_price = float(current_price)

                    if snapshot.get("best_bid") is None and snapshot.get("best_ask") is None:
                        snapshot["best_bid"] = current_price
                        snapshot["best_ask"] = current_price
                    if snapshot.get("mid_price") is None and snapshot.get("best_bid") is not None and snapshot.get("best_ask") is not None:
                        snapshot["mid_price"] = (float(snapshot["best_bid"]) + float(snapshot["best_ask"])) / 2.0

                    qty_shares = (float(pos.our_size) / current_price) if current_price > 0 else 0.0
                    if measurement_mode and pos.action == "BUY":
                        open_exposure = sum(float(p.get("size") or 0.0) for p in db.get_open_positions())
                        if open_exposure + float(pos.our_size) > measurement_total_exposure_cap:
                            logger.warning(
                                "Measurement guardrail: skipping BUY %s due to exposure cap (open=%.2f cap=%.2f)",
                                pos.market,
                                open_exposure,
                                measurement_total_exposure_cap,
                            )
                            continue
                    order_id = f"{run_id}-{trades_count + 1}"
                    if measurement_mode:
                        synthetic_ref_price = float(snapshot.get("mid_price") or current_price)
                        baseline = build_synthetic_baseline(decision_ts, synthetic_ref_price)
                        whale_timestamp = baseline["whale_signal_ts"]
                        whale_entry_ref_price = baseline["whale_entry_ref_price"]
                        whale_ref_type = baseline["whale_ref_type"]
                    else:
                        whale_timestamp = getattr(target_pos, "timestamp", None) if target_pos else None
                        whale_entry_ref_price = float(getattr(target_pos, "avg_price", 0) or 0) if target_pos else None
                        whale_ref_type = "avg_fill" if whale_entry_ref_price else "unknown"
                    order_payload: Dict[str, Any] = {
                        "run_id": run_id,
                        "order_id": order_id,
                        "trade_id": None,
                        "market_id": pos.market,
                        "market_slug": slug_map.get(pos.market, ""),
                        "side": action_side,
                        "order_type": order_type,
                        "qty_shares": qty_shares,
                        "intended_limit_price": current_price if order_type == "limit" else None,
                        "time_in_force": time_in_force,
                        "whale_signal_ts": whale_timestamp,
                        "whale_entry_ref_price": whale_entry_ref_price if whale_entry_ref_price and whale_entry_ref_price > 0 else None,
                        "whale_ref_type": whale_ref_type,
                        "our_decision_ts": decision_ts,
                        "order_sent_ts": datetime.now(UTC),
                        "exchange_ack_ts": datetime.now(UTC),
                        "fill_ts": None,
                        "best_bid": snapshot.get("best_bid"),
                        "best_ask": snapshot.get("best_ask"),
                        "mid_price": snapshot.get("mid_price"),
                        "depth_bid_1": snapshot.get("depth_bid_1"),
                        "depth_ask_1": snapshot.get("depth_ask_1"),
                        "depth_bid_2": None,
                        "depth_ask_2": None,
                        "last_trade_price": snapshot.get("last_trade_price"),
                        "fill_price": None,
                        "fill_price_source": "placeholder",
                        "entry_price_source": current_price_source,
                        "current_price_source": current_price_source,
                        "exit_price_source": "unknown",
                        "filled_shares": None,
                        "fees_usd": 0.0,
                        "is_partial_fill": False,
                        "fill_count": 0,
                        "run_tag": run_tag,
                        "liquidity_tier": measurement_tier_by_market.get(pos.market, "unknown"),
                    }

                    sent_record = diagnostics.record_order_sent(order_payload)
                    if sent_record is None:
                        logger.warning("Skipped invalid slippage diagnostics for %s", pos.market)
                    else:
                        logger.info(
                            "execdiag: recorded order_sent order_id=%s db=%s",
                            sent_record.order_id,
                            db_path,
                        )

                    trade_id: Optional[int] = None
                    if pos.action == "BUY":
                        order_start = time.time()
                        # Record new buy position with outcome from target
                        trade_id = db.add_trade(
                            market=pos.market,
                            side=pos.action,
                            size=pos.our_size,
                            price=current_price,
                            target_wallet=wallet,
                            market_slug=slug_map.get(pos.market, ""),
                            outcome=outcome_map.get(pos.market, "YES"),
                            entry_price_source=current_price_source,
                            current_price_source=current_price_source,
                        )
                        if (time.time() - order_start) > measurement_order_timeout_s:
                            raise TimeoutError(f"Measurement BUY timeout for {pos.market}")
                    elif pos.action == "SELL":
                        order_start = time.time()
                        # Close existing position and realize PnL
                        existing_position = db.get_position_by_market(pos.market)
                        if existing_position:
                            trade_id = int(existing_position["id"])
                            exit_source = "placeholder" if current_price_source == "placeholder" else ("fill" if not dry_run else current_price_source)
                            fill_source = "placeholder" if current_price_source == "placeholder" else ("fill" if not dry_run else current_price_source)
                            pnl = db.close_trade(
                                existing_position['id'],
                                current_price,
                                close_size=pos.our_size,
                                exit_price_source=exit_source,
                                fill_price_source=fill_source,
                            )
                            logger.info(f"Closed position {pos.market[:16]}... PnL: {pnl:+.2f}")
                            # Record loss for risk management cooldown
                            if pnl < 0:
                                risk_mgr.record_loss(abs(pnl))
                        else:
                            logger.warning(f"No open position found for {pos.market}, skipping SELL")
                        if (time.time() - order_start) > measurement_order_timeout_s:
                            raise TimeoutError(f"Measurement SELL timeout for {pos.market}")

                    trades_count += 1
                    market_last_rebalance_ts[pos.market] = loop_time

                    if trade_id is not None:
                        order_payload.update(
                            {
                                "trade_id": trade_id,
                                "fill_ts": datetime.now(UTC),
                                "fill_price": current_price,
                                "fill_price_source": "fill" if not dry_run else current_price_source,
                                "entry_price_source": current_price_source,
                                "current_price_source": current_price_source,
                                "exit_price_source": "fill" if (pos.action == "SELL" and not dry_run) else "unknown",
                                "filled_shares": qty_shares,
                                "is_partial_fill": False,
                                "fill_count": 1,
                            }
                        )
                        fill_record = diagnostics.record_fill(order_payload)
                        if fill_record:
                            if measurement_mode:
                                measurement_fills += 1
                            logger.info(
                                "execdiag: recorded fill order_id=%s fill_price=%s",
                                fill_record.order_id,
                                f"{fill_record.fill_price:.8f}" if fill_record.fill_price is not None else "-",
                            )
                            logger.info(
                                "Slippage diagnostics %s: latency_ms=%s quote_slippage_pct=%s baseline_slippage_pct=%s",
                                pos.market,
                                f"{fill_record.latency_ms:.2f}" if fill_record.latency_ms is not None else "-",
                                f"{fill_record.quote_slippage_pct:.8f}" if fill_record.quote_slippage_pct is not None else "-",
                                f"{fill_record.baseline_slippage_pct:.8f}" if fill_record.baseline_slippage_pct is not None else "-",
                            )
                            if measurement_mode and measurement_fills >= measurement_trades:
                                logger.info("Measurement completed after %d fills", measurement_fills)
                                shutdown_event.set()
                                break

                    # Send trade notification
                    notifier.send_trade_alert(
                        action=pos.action,
                        market=pos.market,
                        size=pos.our_size,
                        price=current_price,
                        dry_run=dry_run,
                    )

            # Update portfolio stats from reconciled ledger values.
            cash_balance = db.get_cash_balance()
            reconciliation = db.reconcile_portfolio(starting_equity=budget)
            total_value = reconciliation.get("total_value", cash_balance)
            positions_cost_basis = sum(p.get('size', 0) for p in our_positions)
            unrealized_pnl = reconciliation.get("total_unrealized", 0.0)
            pnl_24h = db.calculate_24h_pnl()
            db.update_portfolio(total_value, cash_balance, pnl_24h)
            db.run_reconciliation_gate(mode=mode, eps=recon_eps)

            # Calculate P&L % for both us and whale for time-series tracking
            # Our P&L % based on positions cost basis
            our_total_invested = positions_cost_basis
            our_pnl_pct = (unrealized_pnl / our_total_invested * 100) if our_total_invested > 0 else 0

            # Whale P&L % from target positions
            whale_total_invested = sum(
                (p.size * p.avg_price) for p in target_positions
                if p.avg_price and p.size
            )
            whale_total_pnl = sum(p.pnl or 0 for p in target_positions)
            whale_pnl_pct = (whale_total_pnl / whale_total_invested * 100) if whale_total_invested > 0 else 0

            # Record P&L snapshot for time-series comparison
            db.record_pnl_snapshot(
                our_pnl_pct=our_pnl_pct,
                whale_pnl_pct=whale_pnl_pct,
                our_total_invested=our_total_invested,
                whale_total_invested=whale_total_invested,
            )

            # Sync to Google Sheets dashboard
            if sheets_sync:
                trades_for_sync = db.get_all_trades()
                sheets_sync.sync_all(
                    config=config,
                    portfolio_stats=db.get_portfolio_stats(),
                    target_positions=target_positions,
                    our_trades=trades_for_sync,
                    trade_stats=db.get_trade_statistics(),
                    unrealized_pnl=unrealized_pnl,
                    pnl_history=db.get_pnl_history_sampled(hours=48, interval_hours=5),
                    execution_diagnostics=diagnostics.get_recent(limit=500),
                )

            # Update health status
            health_status.update(
                positions_count=len(our_positions),
                trades_count=trades_count,
            )

            # Wait with interruptibility
            shutdown_event.wait(check_interval)

    except Exception as e:
        if measurement_mode:
            logger.exception("Measurement mode aborted due to exception: %s", e)
            debug_info = diagnostics.get_debug_info()
            logger.error(
                "Measurement debug: db=%s table_exists=%s row_count=%s",
                debug_info.get("db_path"),
                debug_info.get("table_exists"),
                debug_info.get("row_count"),
            )
            sample_rows = debug_info.get("sample_rows") or []
            for row in sample_rows:
                logger.error("Measurement debug row: %s", row)
            shutdown_event.set()
        else:
            raise

    finally:
        # Cleanup resources
        console.print("\n[bold yellow]Shutting down copy trader...[/bold yellow]")

        # Send shutdown notification
        final_pnl = db.get_portfolio_stats().get('pnl_total', 0)
        notifier.send_shutdown_notification(
            reason="Signal received",
            final_pnl=final_pnl,
        )

        health_server.stop()
        tracker.close()
        if sheets_sync:
            sheets_sync.close()
        db.close()
        console.print("[green]Cleanup complete.[/green]")
        sys.exit(0)


@cli.command()
@click.option('--health-port', type=int, default=8080, help='Port for health check server')
@click.pass_context
def run(ctx, health_port: int):
    """Run copy trader using environment variables (for Cloud Run).

    Reads configuration from environment variables:
    - COPY_TRADER_TARGET_WALLET (required)
    - COPY_TRADER_BUDGET (required)
    - COPY_TRADER_DRY_RUN (default: true)
    """
    import os
    config = ctx.obj['config']

    # Get wallet from environment
    wallet = os.getenv('COPY_TRADER_TARGET_WALLET') or config.get('target_wallet', '')
    if not wallet:
        console.print("[red]Error: COPY_TRADER_TARGET_WALLET environment variable is required[/red]")
        sys.exit(1)

    # Get budget from environment
    budget_str = os.getenv('COPY_TRADER_BUDGET') or str(config.get('starting_budget', 0))
    try:
        budget = float(budget_str)
        if budget <= 0:
            raise ValueError("Budget must be positive")
    except ValueError as e:
        console.print(f"[red]Error: Invalid COPY_TRADER_BUDGET: {e}[/red]")
        sys.exit(1)

    # Get dry_run from environment
    dry_run_str = os.getenv('COPY_TRADER_DRY_RUN', 'true').lower()
    dry_run = dry_run_str in ('true', '1', 'yes', 'on')
    measurement_mode_str = os.getenv("COPY_TRADER_MEASUREMENT_MODE", "false").lower()
    measurement_mode = measurement_mode_str in ("true", "1", "yes", "on")
    guardrails_str = os.getenv("COPY_TRADER_ENABLE_EXECUTION_GUARDRAILS", "false").lower()
    config.setdefault("execution", {})["enable_execution_guardrails"] = guardrails_str in ("true", "1", "yes", "on")
    run_tag = os.getenv("COPY_TRADER_RUN_TAG") or ("measurement" if measurement_mode else "default")
    config.setdefault("execution", {})["run_tag"] = run_tag
    try:
        measurement_trades = int(os.getenv("COPY_TRADER_MEASUREMENT_TRADES", "30"))
        measurement_max_size_usd = float(os.getenv("COPY_TRADER_MEASUREMENT_MAX_SIZE_USD", "5"))
    except ValueError as e:
        console.print(f"[red]Error: Invalid measurement env vars: {e}[/red]")
        sys.exit(1)
    measurement_market_filter = os.getenv("COPY_TRADER_MEASUREMENT_MARKET_FILTER", "all")

    # Invoke the copy command with these parameters
    ctx.invoke(
        copy,
        wallet=wallet,
        budget=budget,
        dry_run=dry_run,
        health_port=health_port,
        measurement_mode=measurement_mode,
        measurement_trades=measurement_trades,
        measurement_max_size_usd=measurement_max_size_usd,
        measurement_market_filter=measurement_market_filter,
        run_tag=run_tag,
    )


@cli.command()
@click.pass_context
def status(ctx):
    """Show current copy trader status."""
    db = Database()
    positions = db.get_open_positions()
    
    console.print("\n[bold blue]Current Positions[/bold blue]")
    if not positions:
        console.print("No open positions.")
    else:
        for p in positions:
            console.print(f"  {p['market']}: {p['side']} {format_currency(p['size'])}")


@cli.command()
@click.argument('wallet')
@click.pass_context
def watch(ctx, wallet: str):
    """Monitor a wallet without copying."""
    try:
        wallet = validate_wallet_address(wallet)
    except InvalidWalletAddressError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)

    tracker = WalletTracker(wallet)
    
    console.print(f"\n[bold blue]Watching {truncate_address(wallet)}...[/bold blue]\n")
    
    try:
        positions = tracker.get_positions()
        portfolio_value = tracker.get_portfolio_value()
        
        console.print(f"Portfolio Value: [green]{format_currency(portfolio_value)}[/green]")
        console.print(f"Positions: {len(positions)}\n")
        
        for p in positions:
            console.print(f"  {p.market_slug or p.market}")
            console.print(f"    Side: {p.outcome}")
            console.print(f"    Size: {format_currency(p.value)} ({p.size:.4f} shares)")
            console.print(f"    Avg Price: {p.avg_price:.2f}")
            if p.pnl != 0:
                color = "green" if p.pnl > 0 else "red"
                console.print(f"    P&L: [{color}]{format_currency(p.pnl)}[/{color}]")
            console.print()
            
    except RuntimeError as e:
        console.print(f"[red]Error: {e}[/red]")


if __name__ == '__main__':
    cli()
