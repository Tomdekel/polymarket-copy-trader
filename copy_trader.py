#!/usr/bin/env python3
"""Polymarket Copy Trader - Main CLI entry point."""
import click
import time
import sys
import signal
import threading
from pathlib import Path
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
@click.pass_context
def copy(ctx, wallet: str, budget: float, dry_run: bool, health_port: int):
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
    tracker = WalletTracker(wallet)
    sizer = PositionSizer(budget, config)
    risk_mgr = RiskManager(config)
    risk_mgr.set_starting_budget(budget)

    # Display startup info
    console.print("\n[bold blue]Polymarket Copy Trader[/bold blue]")
    console.print(f"Target Wallet: [cyan]{truncate_address(wallet)}[/cyan]")
    console.print(f"Your Budget: [green]{format_currency(budget)}[/green]")
    console.print(f"Mode: [yellow]{'DRY RUN (simulation)' if dry_run else 'LIVE'}[/yellow]")
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
            # Also store outcome for fallback price fetching
            price_map = {pos.market: pos.current_price for pos in target_positions}
            outcome_map = {pos.market: pos.outcome for pos in target_positions}

            # Get our current positions and portfolio stats
            our_positions = db.get_open_positions()
            portfolio_stats = db.get_portfolio_stats()

            # Calculate current PnL from open positions
            current_pnl = sum(p.get('pnl', 0) or 0 for p in our_positions)
            current_pnl += portfolio_stats.get('pnl_total', 0)
            risk_check = risk_mgr.check_risk(current_pnl)

            if not risk_check['allow_trade']:
                logger.warning(f"Risk halt: {risk_check['reason']}")
                notifier.send_risk_alert(
                    reason=risk_check['reason'],
                    current_pnl=current_pnl,
                    daily_pnl=db.calculate_24h_pnl(),
                )
                shutdown_event.wait(check_interval)
                continue

            # Calculate position sizes
            sized_positions = sizer.calculate_positions(
                target_value,
                [{'market': p.market, 'size': p.size, 'value': p.value} for p in target_positions],
                our_positions
            )

            # Execute trades
            for pos in sized_positions:
                if shutdown_event.is_set():
                    break

                if pos.action == "HOLD":
                    continue

                msg = f"{pos.action}: {pos.market} | Target: {pos.target_percentage:.1%} | Our size: {format_currency(pos.our_size)}"

                if dry_run:
                    logger.info(f"[DRY RUN] {msg}")
                else:
                    logger.info(msg)
                    # TODO: Execute actual trade via executor

                # Record trade with current market price
                if pos.action != "HOLD":
                    # Get price from target positions (already fetched from API)
                    current_price = price_map.get(pos.market)

                    # If price missing, try to fetch via CLOB API
                    if current_price is None:
                        outcome = outcome_map.get(pos.market, 'YES')
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

                    if pos.action == "BUY":
                        # Record new buy position
                        db.add_trade(
                            market=pos.market,
                            side=pos.action,
                            size=pos.our_size,
                            price=current_price,
                            target_wallet=wallet
                        )
                    elif pos.action == "SELL":
                        # Close existing position and realize PnL
                        existing_position = db.get_position_by_market(pos.market)
                        if existing_position:
                            pnl = db.close_trade(existing_position['id'], current_price)
                            logger.info(f"Closed position {pos.market[:16]}... PnL: {pnl:+.2f}")
                        else:
                            logger.warning(f"No open position found for {pos.market}, skipping SELL")

                    trades_count += 1

                    # Send trade notification
                    notifier.send_trade_alert(
                        action=pos.action,
                        market=pos.market,
                        size=pos.our_size,
                        price=current_price,
                        dry_run=dry_run,
                    )

            # Update portfolio stats
            cash_balance = db.get_cash_balance()
            positions_value = sum(p.get('size', 0) * p.get('price', 1) for p in our_positions)
            total_value = cash_balance + positions_value
            pnl_24h = db.calculate_24h_pnl()
            db.update_portfolio(total_value, cash_balance, pnl_24h, current_pnl)

            # Sync to Google Sheets dashboard
            if sheets_sync:
                sheets_sync.sync_all(
                    config=config,
                    portfolio_stats=db.get_portfolio_stats(),
                    target_positions=target_positions,
                    our_trades=db.get_recent_trades(limit=100),
                    trade_stats=db.get_trade_statistics(),
                )

            # Update health status
            health_status.update(
                positions_count=len(our_positions),
                trades_count=trades_count,
            )

            # Wait with interruptibility
            shutdown_event.wait(check_interval)

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

    # Invoke the copy command with these parameters
    ctx.invoke(copy, wallet=wallet, budget=budget, dry_run=dry_run, health_port=health_port)


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