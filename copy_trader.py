#!/usr/bin/env python3
"""Polymarket Copy Trader - Main CLI entry point."""
import yaml
import click
import time
import sys
from pathlib import Path
from typing import Dict, Any

from utils import setup_logging, console, format_currency, truncate_address
from wallet_tracker import WalletTracker
from database import Database
from position_sizer import PositionSizer
from risk_manager import RiskManager

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--config', '-c', type=click.Path(), default='config.yaml',
              help='Path to config file')
@click.pass_context
def cli(ctx, config):
    """Polymarket Copy Trader - Copy trades from any wallet."""
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = config
    
    # Load config
    with open(config) as f:
        ctx.obj['config'] = yaml.safe_load(f)
    
    # Setup logging
    log_level = ctx.obj['config'].get('reporting', {}).get('log_level', 'INFO')
    ctx.obj['logger'] = setup_logging(log_level)


@cli.command()
@click.option('--wallet', '-w', required=True, help='Target wallet address to copy')
@click.option('--budget', '-b', type=float, required=True, help='Your trading budget in USD')
@click.option('--dry-run/--live', default=True, help='Run in simulation mode (default) or live')
@click.pass_context
def copy(ctx, wallet: str, budget: float, dry_run: bool):
    """Start copying trades from a target wallet."""
    config = ctx.obj['config']
    logger = ctx.obj['logger']
    
    # Override config with CLI args
    config['target_wallet'] = wallet
    config['starting_budget'] = budget
    config['execution']['dry_run'] = dry_run
    
    # Initialize components
    db = Database()
    tracker = WalletTracker(wallet)
    sizer = PositionSizer(budget, config)
    risk_mgr = RiskManager(config)
    risk_mgr.set_starting_budget(budget)
    
    # Display startup info
    console.print("\n[bold blue]Polymarket Copy Trader[/bold blue]")
    console.print(f"Target Wallet: [cyan]{truncate_address(wallet)}[/cyan]")
    console.print(f"Your Budget: [green]{format_currency(budget)}[/green]")
    console.print(f"Mode: [yellow]{'DRY RUN (simulation)' if dry_run else 'LIVE'}[/yellow]\n")
    
    check_interval = config['execution'].get('check_interval', 30)
    
    try:
        while True:
            # Get target wallet positions
            try:
                target_positions = tracker.get_positions()
                target_value = tracker.get_portfolio_value()
            except RuntimeError as e:
                logger.error(f"Failed to fetch target wallet: {e}")
                time.sleep(check_interval)
                continue
            
            # Get our current positions
            our_positions = db.get_open_positions()
            
            # Check risk limits
            current_pnl = sum(p.get('pnl', 0) for p in our_positions)
            risk_check = risk_mgr.check_risk(current_pnl)
            
            if not risk_check['allow_trade']:
                logger.warning(f"Risk halt: {risk_check['reason']}")
                time.sleep(check_interval)
                continue
            
            # Calculate position sizes
            sized_positions = sizer.calculate_positions(
                target_value, 
                [{'market': p.market, 'size': p.size, 'value': p.value} for p in target_positions],
                our_positions
            )
            
            # Execute trades
            for pos in sized_positions:
                if pos.action == "HOLD":
                    continue
                
                msg = f"{pos.action}: {pos.market} | Target: {pos.target_percentage:.1%} | Our size: {format_currency(pos.our_size)}"
                
                if dry_run:
                    logger.info(f"[DRY RUN] {msg}")
                else:
                    logger.info(msg)
                    # TODO: Execute actual trade via executor
                
                # Record trade
                if pos.action != "HOLD":
                    db.add_trade(
                        market=pos.market,
                        side=pos.action,
                        size=pos.our_size,
                        price=0,  # Will be filled by executor
                        target_wallet=wallet
                    )
            
            # Update portfolio stats
            total_value = budget + current_pnl
            db.update_portfolio(total_value, budget - sum(p['size'] for p in our_positions), 
                               0, current_pnl)
            
            time.sleep(check_interval)
            
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Copy trader stopped.[/bold yellow]")
        sys.exit(0)


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