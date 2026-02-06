"""Google Sheets sync for Polymarket Copy Trader dashboard."""
import json
import os
import logging
from datetime import datetime
from threading import Lock
from typing import Dict, Any, List, Optional

from pnl import (
    assert_shares_consistent,
    compute_cost_basis,
    compute_current_value,
    compute_proceeds,
    compute_realized_pnl,
    compute_shares,
    compute_unrealized_pnl,
    reconcile_trade_ledger,
    validate_trade_field_semantics,
)

logger = logging.getLogger(__name__)

# Tab names
TAB_PORTFOLIO = "Portfolio Summary"
TAB_TARGET_POSITIONS = "Target Positions"
TAB_OUR_TRADES = "Our Trades"
TAB_COMPARISON = "Comparison"
TAB_EXECUTION_DIAGNOSTICS = "Execution Diagnostics"


class GoogleSheetsSync:
    """Sync copy trader data to Google Sheets for dashboard display."""

    def __init__(self, sheet_id: str, credentials_path: str):
        """Initialize Google Sheets sync.

        Args:
            sheet_id: Google Sheet ID (from URL)
            credentials_path: Path to service account JSON file
        """
        self.sheet_id = sheet_id
        self.credentials_path = credentials_path
        self._client = None
        self._sheet = None
        self._last_sync: Optional[datetime] = None
        self._min_sync_interval = 180  # Minimum seconds between syncs (3 minutes)
        self._lock = Lock()

    def _get_client(self):
        """Lazy-load gspread client."""
        if self._client is None:
            try:
                import gspread
                from google.oauth2.service_account import Credentials
            except ImportError:
                raise ImportError(
                    "gspread and google-auth are required for Google Sheets sync. "
                    "Install with: pip install gspread google-auth"
                )

            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_file(
                self.credentials_path, scopes=scopes
            )
            self._client = gspread.authorize(creds)

        return self._client

    def _get_sheet(self):
        """Get the Google Sheet, creating tabs if needed."""
        if self._sheet is None:
            client = self._get_client()
            self._sheet = client.open_by_key(self.sheet_id)
            self._ensure_tabs_exist()
        return self._sheet

    def _ensure_tabs_exist(self) -> None:
        """Ensure all required tabs exist in the sheet."""
        sheet = self._sheet
        existing_tabs = [ws.title for ws in sheet.worksheets()]

        # Create missing tabs
        for tab_name in [TAB_PORTFOLIO, TAB_TARGET_POSITIONS, TAB_OUR_TRADES, TAB_COMPARISON, TAB_EXECUTION_DIAGNOSTICS]:
            if tab_name not in existing_tabs:
                sheet.add_worksheet(title=tab_name, rows=200, cols=20)
                logger.info(f"Created sheet tab: {tab_name}")

    def _format_currency(self, value: float) -> str:
        """Format value as currency string."""
        if value is None:
            return "$0.00"
        if value >= 0:
            return f"${value:,.2f}"
        return f"-${abs(value):,.2f}"

    def _format_pnl(self, value: float) -> str:
        """Format PnL value.

        Note: Don't use "+" prefix as Google Sheets USER_ENTERED mode
        interprets it as a formula operator, causing #ERROR!.
        """
        if value is None:
            return "$0.00"
        if value >= 0:
            return f"${value:,.2f}"
        return f"-${abs(value):,.2f}"

    def _format_percentage(self, value: float) -> str:
        """Format value as percentage."""
        return f"{value:.2%}"

    def _format_pnl_percentage(self, value: float, decimals: int = 1) -> str:
        """Format P&L percentage without leading + that breaks Sheets.

        Google Sheets USER_ENTERED mode interprets '+' as a formula operator,
        causing #ERROR!. This method formats percentages safely.

        Args:
            value: The percentage value (already multiplied by 100)
            decimals: Number of decimal places (default 1)

        Returns:
            Formatted percentage string like "5.0%" or "-3.2%"
        """
        if value is None:
            return "-"
        if decimals == 1:
            return f"{value:.1f}%"
        elif decimals == 2:
            return f"{value:.2f}%"
        else:
            return f"{value:.{decimals}f}%"

    def _format_market_link(self, market_slug: str, market_id: str = "") -> str:
        """Format market name as hyperlink to Polymarket.

        Args:
            market_slug: Human-readable market name/slug
            market_id: Market ID (condition ID) - used if slug is empty

        Returns:
            Google Sheets hyperlink formula or plain text
        """
        # Clean up slug - treat 'None' string as empty
        if market_slug in (None, 'None', ''):
            market_slug = ""

        # Use slug for display, fallback to truncated market_id
        if market_slug:
            display_name = market_slug
        elif market_id:
            display_name = market_id[:30] + "..." if len(market_id) > 30 else market_id
        else:
            return "Unknown"

        # Escape quotes in display name for the formula
        display_name_escaped = display_name.replace('"', '""')

        # Build URL - use condition ID for reliable linking
        # Polymarket URLs work with condition IDs: polymarket.com/event/[slug] or /markets/[conditionId]
        if market_id:
            url = f"https://polymarket.com/markets/{market_id}"
        elif market_slug:
            url_slug = market_slug.lower().replace(" ", "-")
            url = f"https://polymarket.com/event/{url_slug}"
        else:
            return display_name

        # Google Sheets HYPERLINK formula
        return f'=HYPERLINK("{url}", "{display_name_escaped}")'

    def _format_duration(self, start_time: str) -> str:
        """Format duration since start time as 'Xh Ym' or 'Xd Yh'.

        Args:
            start_time: ISO format timestamp string

        Returns:
            Human-readable duration string
        """
        try:
            start = datetime.fromisoformat(start_time)
            delta = datetime.now() - start
            total_seconds = int(delta.total_seconds())

            if total_seconds < 0:
                return "0m"

            days, remainder = divmod(total_seconds, 86400)
            hours, remainder = divmod(remainder, 3600)
            minutes = remainder // 60

            if days > 0:
                return f"{days}d {hours}h" if hours > 0 else f"{days}d"
            elif hours > 0:
                return f"{hours}h {minutes}m" if minutes > 0 else f"{hours}h"
            else:
                return f"{minutes}m"
        except (ValueError, TypeError):
            return "Unknown"

    def sync_portfolio(
        self,
        target_wallet: str,
        dry_run: bool,
        initial_budget: float,
        current_value: float,
        cash_available: float,
        pnl_24h: float,
        pnl_total: float,
        whale_profile_url: Optional[str] = None,
        session_started: Optional[str] = None,
        trade_stats: Optional[Dict[str, Any]] = None,
        unrealized_pnl: Optional[float] = None,
    ) -> None:
        """Sync portfolio summary to the Portfolio Summary tab.

        Args:
            target_wallet: Target wallet address being copied
            dry_run: Whether running in dry-run mode
            initial_budget: Initial budget amount
            current_value: Current portfolio value
            cash_available: Available cash balance
            pnl_24h: Profit/loss in last 24 hours (realized)
            pnl_total: Total realized profit/loss (from closed trades only)
            whale_profile_url: URL to whale's Polymarket profile
            session_started: ISO timestamp when session started
            trade_stats: Dictionary of trade statistics from database
            unrealized_pnl: Unrealized P&L from open positions (passed separately)
        """
        sheet = self._get_sheet()
        worksheet = sheet.worksheet(TAB_PORTFOLIO)

        # Build the summary data
        mode = "DRY RUN" if dry_run else "LIVE"
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Format session time and duration
        session_display = "Unknown"
        duration_display = "Unknown"
        if session_started:
            try:
                session_dt = datetime.fromisoformat(session_started)
                session_display = session_dt.strftime("%Y-%m-%d %H:%M:%S")
                duration_display = self._format_duration(session_started)
            except (ValueError, TypeError):
                pass

        # Reconciled open allocation is current portfolio value minus available cash.
        allocated_in_deals = max(current_value - cash_available, 0.0)

        # Use passed unrealized_pnl if provided, otherwise calculate from equity drift.
        if unrealized_pnl is None:
            unrealized_pnl = (current_value - initial_budget) - pnl_total

        # Total P&L = unrealized + realized
        total_pnl = unrealized_pnl + pnl_total

        data = [
            ["Field", "Value"],
            ["Whale Profile", whale_profile_url or f"https://polymarket.com/profile/{target_wallet}"],
            ["Target Wallet", target_wallet],
            ["Mode", mode],
            ["Session Started", session_display],
            ["Trading Duration", duration_display],
            ["", ""],
            ["Initial Budget", self._format_currency(initial_budget)],
            ["Allocated in Deals", self._format_currency(allocated_in_deals)],
            ["Cash Available", self._format_currency(cash_available)],
            ["Current Value", self._format_currency(current_value)],
            ["Unrealized P&L", self._format_pnl(unrealized_pnl)],
            ["Realized P&L (24h)", self._format_pnl(pnl_24h)],
            ["Realized P&L (Total)", self._format_pnl(pnl_total)],
            ["Total P&L", self._format_pnl(total_pnl)],
            ["Last Updated", updated_at],
        ]

        # Add trade statistics if available
        if trade_stats:
            stats = trade_stats
            win_rate = stats.get("win_rate", 0)
            win_rate_display = f"{win_rate:.1f}%" if win_rate else "N/A"

            data.extend([
                ["", ""],
                ["--- DEAL STATISTICS ---", ""],
                ["Total Bets", stats.get("total_trades", 0)],
                ["Open Positions", stats.get("open_trades", 0)],
                ["Closed Positions", stats.get("closed_trades", 0)],
                ["", ""],
                ["Winning Trades", stats.get("winning_trades", 0)],
                ["Losing Trades", stats.get("losing_trades", 0)],
                ["Win Rate", win_rate_display],
                ["", ""],
                ["Average Bet Size", self._format_currency(stats.get("avg_trade_size", 0))],
                ["Average Win", self._format_pnl(stats.get("avg_win", 0))],
                ["Average Loss", self._format_pnl(stats.get("avg_loss", 0))],
                ["Largest Win", self._format_pnl(stats.get("largest_win", 0))],
                ["Largest Loss", self._format_pnl(stats.get("largest_loss", 0))],
            ])

        # Clear and update in one batch
        worksheet.clear()
        worksheet.update(range_name="A1", values=data)
        logger.debug("Synced portfolio summary to Google Sheets")

    def sync_target_positions(self, positions: List[Dict[str, Any]]) -> None:
        """Sync target wallet positions to the Target Positions tab.

        Args:
            positions: List of target wallet positions with keys:
                - market: Market identifier (UUID/condition ID)
                - market_slug: Human-readable market name
                - outcome: YES or NO
                - size: Number of shares
                - avg_price: Average entry price
                - current_price: Current market price
                - value: Current position value
                - pnl: Unrealized P&L
        """
        sheet = self._get_sheet()
        worksheet = sheet.worksheet(TAB_TARGET_POSITIONS)

        # Header row - unified structure matching Our Trades
        headers = ["Market", "Side", "Shares", "Cost Basis", "Current Value", "Entry Price", "Current Price", "P&L", "P&L %", "Status"]
        data = [headers]

        # Add position rows
        for pos in positions:
            shares = pos.get('size') or 0  # Number of shares
            avg_price = pos.get('avg_price') or 0
            current_price = pos.get('current_price') or 0
            value = pos.get('value') or 0  # Current market value
            pnl = pos.get('pnl') or 0
            outcome = pos.get("outcome") or "YES"  # Side (YES/NO)
            market_slug = pos.get("market_slug") or pos.get("market") or ""
            market_id = pos.get("market") or ""

            # Calculate cost basis = shares × avg_price
            cost_basis = shares * avg_price

            # Calculate P&L % based on cost basis
            if cost_basis > 0:
                pnl_pct = (pnl / cost_basis) * 100
                pnl_pct_str = self._format_pnl_percentage(pnl_pct)
            else:
                pnl_pct_str = "-"

            row = [
                self._format_market_link(market_slug, market_id),
                outcome,  # Side (YES/NO)
                f"{shares:.4f}",  # Shares
                self._format_currency(cost_basis),  # Cost Basis
                self._format_currency(value),  # Current Value
                f"{avg_price:.4f}",  # Entry Price
                f"{current_price:.4f}",  # Current Price
                self._format_pnl(pnl),
                pnl_pct_str,
                "open",  # Status
            ]
            data.append(row)

        # Clear and update in one batch
        worksheet.clear()
        worksheet.update(range_name="A1", values=data, value_input_option="USER_ENTERED")
        logger.debug(f"Synced {len(positions)} target positions to Google Sheets")

    def sync_our_trades(
        self,
        trades: List[Dict[str, Any]],
        target_positions: Optional[List[Dict[str, Any]]] = None,
        max_trades: int = 500,
    ) -> None:
        """Sync our trades to the Our Trades tab.

        Args:
            trades: List of our trades with keys:
                - timestamp: Trade timestamp
                - market: Market identifier (UUID)
                - market_slug: Human-readable market name
                - side: BUY or SELL
                - size: Trade size in USD
                - price: Trade price (entry)
                - current_price: Current market price (for open trades)
                - sell_price: Exit price (for closed trades)
                - pnl: P&L (unrealized for open, realized for closed)
                - status: Trade status (open/closed)
            target_positions: Target wallet positions to lookup missing market slugs
            max_trades: Maximum number of recent trades to sync (default: 500)
        """
        # Limit to most recent trades to avoid memory issues
        trades = trades[:max_trades] if len(trades) > max_trades else trades
        sheet = self._get_sheet()
        worksheet = sheet.worksheet(TAB_OUR_TRADES)

        # Create slug lookup from target positions for backfilling missing slugs
        slug_lookup: Dict[str, str] = {}
        if target_positions:
            for pos in target_positions:
                market_id = pos.get("market", "")
                slug = pos.get("market_slug", "")
                if market_id and slug:
                    slug_lookup[market_id] = slug

        # Header row - explicit realized/unrealized accounting columns.
        headers = [
            "Market",
            "Side",
            "Shares",
            "Cost Basis",
            "Current Value",
            "Entry Price",
            "Exit Price",
            "Current Price",
            "Proceeds",
            "Realized P&L",
            "Unrealized P&L",
            "P&L",
            "P&L %",
            "Status",
        ]
        data = [headers]

        # Add trade rows
        for trade in trades:
            status = trade.get("status") or "open"
            semantics_errors = validate_trade_field_semantics(trade)
            if semantics_errors:
                raise ValueError("; ".join(semantics_errors))
            size = float(trade.get("size") or 0.0)  # legacy cost basis USD
            entry_price = float(trade.get("price") or 0.0)
            market_id = trade.get("market") or ""

            # Use slug from trade, or look up from target positions
            market_slug = trade.get("market_slug") or ""
            if not market_slug and market_id in slug_lookup:
                market_slug = slug_lookup[market_id]

            # Use outcome from trade record (YES/NO), fallback to YES for legacy trades
            side = trade.get("outcome") or "YES"

            shares = float(trade.get("shares") or 0.0)
            if shares <= 0 and entry_price > 0:
                shares = compute_shares(size, entry_price)
            cost_basis_usd = compute_cost_basis(shares, entry_price)
            if cost_basis_usd <= 0:
                cost_basis_usd = size
            assert_shares_consistent(
                shares=shares,
                entry_price=entry_price,
                cost_basis_usd=cost_basis_usd,
            )

            exit_price = trade.get("sell_price")
            current_price = trade.get("current_price")
            if current_price is None:
                current_price = float(exit_price) if exit_price is not None else entry_price
            current_price = float(current_price)

            if status == "closed":
                effective_exit = float(exit_price) if exit_price is not None else None
                proceeds = compute_proceeds(shares, effective_exit)
                realized_pnl = compute_realized_pnl(
                    shares=shares,
                    entry_price=entry_price,
                    exit_price=effective_exit,
                )
                unrealized_pnl = 0.0
                current_value = proceeds
            else:
                current_value = compute_current_value(shares, current_price)
                unrealized_pnl = compute_unrealized_pnl(
                    shares=shares,
                    entry_price=entry_price,
                    current_price=current_price,
                )
                realized_pnl = 0.0
                proceeds = 0.0

            pnl_value = unrealized_pnl if status == "open" else realized_pnl
            pnl_pct_str = self._format_pnl_percentage((pnl_value / cost_basis_usd) * 100) if cost_basis_usd > 0 else "-"

            row = [
                self._format_market_link(market_slug, market_id),
                side,
                f"{shares:.4f}",
                self._format_currency(cost_basis_usd),
                self._format_currency(current_value),
                f"{entry_price:.4f}",
                f"{float(exit_price):.4f}" if exit_price is not None else "-",
                f"{current_price:.4f}",
                self._format_currency(proceeds) if status == "closed" else "-",
                self._format_pnl(realized_pnl),
                self._format_pnl(unrealized_pnl),
                self._format_pnl(pnl_value),
                pnl_pct_str,
                status,
            ]
            data.append(row)

        # Clear and update in one batch
        worksheet.clear()
        worksheet.update(range_name="A1", values=data, value_input_option="USER_ENTERED")
        logger.debug(f"Synced {len(trades)} trades to Google Sheets")

    def sync_comparison(
        self,
        target_positions: List[Dict[str, Any]],
        our_trades: List[Dict[str, Any]],
        trade_stats: Optional[Dict[str, Any]] = None,
        pnl_history: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Sync comparison analysis to the Comparison tab.

        Focuses on SHARED markets only to evaluate copy trading viability.
        Includes a P&L % over time comparison chart.

        Args:
            target_positions: List of target wallet positions
            our_trades: List of our trades (open positions only for comparison)
            trade_stats: Trade statistics from database
            pnl_history: P&L history snapshots for time-series chart
        """
        sheet = self._get_sheet()
        worksheet = sheet.worksheet(TAB_COMPARISON)

        # Filter to only open trades for our positions
        our_open = [t for t in our_trades if t.get("status") == "open"]

        # Build market lookups (keyed by market UUID)
        target_by_market = {pos.get("market"): pos for pos in target_positions if pos.get("market")}
        our_by_market = {t.get("market"): t for t in our_open if t.get("market")}

        target_markets = set(target_by_market.keys())
        our_markets = set(our_by_market.keys())
        shared_markets = target_markets & our_markets
        missing_from_us = target_markets - our_markets
        extra_positions = our_markets - target_markets

        data = []

        # ============================================================
        # Section 1: SHARED POSITIONS ANALYSIS (only markets both have)
        # ============================================================
        data.append(["--- SHARED POSITIONS ANALYSIS ---", "", "", ""])
        data.append(["(Only markets where we copied the whale)", "", "", ""])
        data.append(["", "", "", ""])

        if shared_markets:
            # Calculate totals for shared markets only
            shared_target_invested = 0.0
            shared_target_value = 0.0
            shared_target_pnl = 0.0
            shared_our_invested = 0.0
            shared_our_value = 0.0
            shared_our_pnl = 0.0
            shared_target_winners = 0
            shared_our_winners = 0
            entry_price_diffs = []

            for market in shared_markets:
                target_pos = target_by_market[market]
                our_pos = our_by_market[market]

                # Target calculations
                t_shares = target_pos.get("size") or 0
                t_avg_price = target_pos.get("avg_price") or 0
                t_value = target_pos.get("value") or 0
                t_pnl = target_pos.get("pnl") or 0
                t_cost = t_shares * t_avg_price
                shared_target_invested += t_cost
                shared_target_value += t_value
                shared_target_pnl += t_pnl
                if t_pnl > 0:
                    shared_target_winners += 1

                # Our calculations
                o_cost = our_pos.get("size") or 0  # cost basis
                o_entry_price = our_pos.get("price") or 0
                o_current_price = our_pos.get("current_price") or o_entry_price
                o_pnl = our_pos.get("pnl") or 0
                o_shares = o_cost / o_entry_price if o_entry_price > 0 else 0
                o_value = o_shares * o_current_price
                shared_our_invested += o_cost
                shared_our_value += o_value
                shared_our_pnl += o_pnl
                if o_pnl > 0:
                    shared_our_winners += 1

                # Entry price comparison (slippage)
                if t_avg_price > 0:
                    entry_diff_pct = ((o_entry_price - t_avg_price) / t_avg_price) * 100
                    entry_price_diffs.append(entry_diff_pct)

            # Calculate P&L percentages
            target_pnl_pct = (shared_target_pnl / shared_target_invested * 100) if shared_target_invested > 0 else 0
            our_pnl_pct = (shared_our_pnl / shared_our_invested * 100) if shared_our_invested > 0 else 0
            target_win_pct = (shared_target_winners / len(shared_markets) * 100) if shared_markets else 0
            our_win_pct = (shared_our_winners / len(shared_markets) * 100) if shared_markets else 0
            avg_slippage = sum(entry_price_diffs) / len(entry_price_diffs) if entry_price_diffs else 0

            data.append(["Metric", "Target (Whale)", "Ours", "Difference"])
            data.append([
                "Shared Positions",
                len(shared_markets),
                len(shared_markets),
                "0 (same markets)",
            ])
            data.append([
                "Total Invested",
                self._format_currency(shared_target_invested),
                self._format_currency(shared_our_invested),
                self._format_pnl(shared_our_invested - shared_target_invested),
            ])
            data.append([
                "Current Value",
                self._format_currency(shared_target_value),
                self._format_currency(shared_our_value),
                self._format_pnl(shared_our_value - shared_target_value),
            ])
            data.append([
                "Total P&L ($)",
                self._format_pnl(shared_target_pnl),
                self._format_pnl(shared_our_pnl),
                self._format_pnl(shared_our_pnl - shared_target_pnl),
            ])
            data.append([
                "Total P&L (%)",
                self._format_pnl_percentage(target_pnl_pct),
                self._format_pnl_percentage(our_pnl_pct),
                self._format_pnl_percentage(our_pnl_pct - target_pnl_pct),
            ])
            data.append([
                "Winning Positions",
                f"{shared_target_winners} ({target_win_pct:.0f}%)",
                f"{shared_our_winners} ({our_win_pct:.0f}%)",
                f"{shared_our_winners - shared_target_winners}",
            ])
            data.append([
                "Avg Entry Slippage",
                "-",
                self._format_pnl_percentage(avg_slippage, decimals=2),
                "(vs whale entry price)",
            ])
        else:
            data.append(["No shared positions yet - need to copy some trades first", "", "", ""])

        data.append(["", "", "", ""])

        # ============================================================
        # Section 2: PER-MARKET COMPARISON (shared markets only)
        # ============================================================
        data.append(["--- PER-MARKET COMPARISON (Shared Only) ---", "", "", "", "", "", "", "", "", ""])
        data.append([
            "Market",
            "Side",
            "Target Entry",
            "Our Entry",
            "Entry Diff",
            "Target P&L",
            "Our P&L",
            "P&L Diff",
            "Target P&L%",
            "Our P&L%",
        ])

        for market in sorted(shared_markets):
            target_pos = target_by_market[market]
            our_pos = our_by_market[market]

            # Target data
            t_side = target_pos.get("outcome", "YES")
            t_avg_price = target_pos.get("avg_price") or 0
            t_shares = target_pos.get("size") or 0
            t_pnl = target_pos.get("pnl") or 0
            t_cost = t_shares * t_avg_price
            t_pnl_pct = (t_pnl / t_cost * 100) if t_cost > 0 else 0

            # Our data - use outcome from trade record, fallback to YES
            o_side = our_pos.get("outcome") or "YES"
            o_entry_price = our_pos.get("price") or 0
            o_cost = our_pos.get("size") or 0
            o_pnl = our_pos.get("pnl") or 0
            o_pnl_pct = (o_pnl / o_cost * 100) if o_cost > 0 else 0

            # Entry price difference
            if t_avg_price > 0:
                entry_diff = ((o_entry_price - t_avg_price) / t_avg_price) * 100
                entry_diff_str = self._format_pnl_percentage(entry_diff, decimals=2)
            else:
                entry_diff_str = "-"

            # Get market_slug for display
            market_slug = target_pos.get("market_slug", "") or our_pos.get("market_slug", "")
            market_display = self._format_market_link(market_slug, market)

            data.append([
                market_display,
                f"{t_side} / {o_side}",
                f"{t_avg_price:.4f}",
                f"{o_entry_price:.4f}",
                entry_diff_str,
                self._format_pnl(t_pnl),
                self._format_pnl(o_pnl),
                self._format_pnl(o_pnl - t_pnl),
                self._format_pnl_percentage(t_pnl_pct),
                self._format_pnl_percentage(o_pnl_pct),
            ])

        if not shared_markets:
            data.append(["No shared positions to compare", "", "", "", "", "", "", "", "", ""])

        data.append(["", "", "", "", "", "", "", "", "", ""])

        # ============================================================
        # Section 3: POSITION GAPS (informational)
        # ============================================================
        data.append(["--- POSITION GAPS ---", "", "", ""])
        data.append(["", "", "", ""])

        # Markets target has that we don't (missed opportunities)
        data.append([f"MISSED OPPORTUNITIES (whale has, we don't): {len(missing_from_us)}", "", "", ""])
        if missing_from_us:
            data.append(["Market", "Side", "Value", "P&L"])
            for market in sorted(missing_from_us):
                target_pos = target_by_market.get(market)
                value = target_pos.get("value", 0) if target_pos else 0
                pnl = target_pos.get("pnl", 0) if target_pos else 0
                side = target_pos.get("outcome", "") if target_pos else ""
                market_slug = target_pos.get("market_slug", "") if target_pos else ""
                market_link = self._format_market_link(market_slug, market)
                data.append([market_link, side, self._format_currency(value), self._format_pnl(pnl)])
        else:
            data.append(["None - we have all whale positions!", "", "", ""])

        data.append(["", "", "", ""])

        # Markets we have that target doesn't (shouldn't happen if copying)
        data.append([f"EXTRA POSITIONS (we have, whale doesn't): {len(extra_positions)}", "", "", ""])
        if extra_positions:
            data.append(["Market", "Side", "Cost Basis", "P&L"])
            for market in sorted(extra_positions):
                our_pos = our_by_market.get(market)
                cost = our_pos.get("size", 0) if our_pos else 0
                pnl = our_pos.get("pnl", 0) if our_pos else 0
                side = our_pos.get("outcome", "YES") if our_pos else "YES"
                market_slug = our_pos.get("market_slug", "") if our_pos else ""
                market_link = self._format_market_link(market_slug, market)
                data.append([market_link, side, self._format_currency(cost), self._format_pnl(pnl)])
        else:
            data.append(["None - all our positions are copies!", "", "", ""])

        data.append(["", "", "", ""])

        # ============================================================
        # Section 4: STRATEGY VIABILITY SUMMARY
        # ============================================================
        data.append(["--- STRATEGY VIABILITY SUMMARY ---", "", ""])
        data.append(["Metric", "Value", "Interpretation"])

        # Coverage
        coverage = len(shared_markets) / len(target_markets) * 100 if target_markets else 0
        if coverage >= 80:
            coverage_interp = "Excellent - copying most trades"
        elif coverage >= 50:
            coverage_interp = "Good - copying majority"
        elif coverage >= 20:
            coverage_interp = "Moderate - missing many opportunities"
        else:
            coverage_interp = "Poor - barely copying"
        data.append([
            "Coverage",
            f"{len(shared_markets)}/{len(target_markets)} ({coverage:.0f}%)",
            coverage_interp,
        ])

        # Entry slippage
        if shared_markets:
            entry_price_diffs = []
            for market in shared_markets:
                target_pos = target_by_market[market]
                our_pos = our_by_market[market]
                t_avg_price = target_pos.get("avg_price") or 0
                o_entry_price = our_pos.get("price") or 0
                if t_avg_price > 0:
                    entry_diff_pct = ((o_entry_price - t_avg_price) / t_avg_price) * 100
                    entry_price_diffs.append(entry_diff_pct)
            avg_slippage = sum(entry_price_diffs) / len(entry_price_diffs) if entry_price_diffs else 0

            if abs(avg_slippage) < 2:
                slippage_interp = "Excellent - nearly identical entries"
            elif abs(avg_slippage) < 5:
                slippage_interp = "Good - minor timing differences"
            elif abs(avg_slippage) < 10:
                slippage_interp = "Moderate - noticeable slippage"
            else:
                slippage_interp = "Poor - significant entry disadvantage"
            data.append([
                "Avg Entry Slippage",
                self._format_pnl_percentage(avg_slippage, decimals=2),
                slippage_interp,
            ])

            # Performance vs whale (on shared positions)
            shared_target_pnl = sum(target_by_market[m].get("pnl", 0) for m in shared_markets)
            shared_our_pnl = sum(our_by_market[m].get("pnl", 0) or 0 for m in shared_markets)
            perf_diff = shared_our_pnl - shared_target_pnl

            if perf_diff > 0:
                perf_interp = "Outperforming whale!"
            elif perf_diff > -10:
                perf_interp = "Tracking whale closely"
            elif perf_diff > -50:
                perf_interp = "Lagging whale moderately"
            else:
                perf_interp = "Significantly underperforming"
            data.append([
                "Performance vs Whale",
                self._format_pnl(perf_diff),
                perf_interp,
            ])

            # Overall viability
            if coverage >= 50 and abs(avg_slippage) < 5:
                viability = "VIABLE - Good copy trading setup"
            elif coverage >= 30 or abs(avg_slippage) < 10:
                viability = "MODERATE - Consider improving coverage/timing"
            else:
                viability = "REVIEW NEEDED - Coverage or slippage issues"
            data.append([
                "Overall Viability",
                viability,
                "",
            ])
        else:
            data.append(["Avg Entry Slippage", "N/A", "No shared positions yet"])
            data.append(["Performance vs Whale", "N/A", "No shared positions yet"])
            data.append(["Overall Viability", "PENDING", "Need to copy some trades first"])

        data.append(["", "", ""])

        # ============================================================
        # Section 5: P&L % OVER TIME CHART
        # ============================================================
        data.append(["--- P&L % OVER TIME (5-hour intervals) ---", "", "", "", ""])
        data.append(["", "", "", "", ""])

        if pnl_history and len(pnl_history) >= 2:
            # Chart data header
            data.append(["Time", "Our P&L %", "Whale P&L %", "Difference", ""])

            # Add chart data rows
            chart_data_start_row = len(data) + 1  # 1-indexed for Sheets
            our_pnl_values = []
            whale_pnl_values = []

            for snapshot in pnl_history:
                try:
                    ts = datetime.fromisoformat(snapshot['timestamp'])
                    time_str = ts.strftime("%m/%d %H:%M")
                except (ValueError, TypeError):
                    time_str = "Unknown"

                our_pnl = snapshot.get('our_pnl_pct') or 0
                whale_pnl = snapshot.get('whale_pnl_pct') or 0
                diff = our_pnl - whale_pnl

                our_pnl_values.append(our_pnl)
                whale_pnl_values.append(whale_pnl)

                data.append([
                    time_str,
                    self._format_pnl_percentage(our_pnl, decimals=2),
                    self._format_pnl_percentage(whale_pnl, decimals=2),
                    self._format_pnl_percentage(diff, decimals=2),
                    "",
                ])

            chart_data_end_row = len(data)

            data.append(["", "", "", "", ""])

            # Add SPARKLINE formulas for visual chart (inline mini-charts)
            # SPARKLINE shows a small line chart in a single cell
            if len(our_pnl_values) >= 2:
                data.append(["Our P&L Trend:", f'=SPARKLINE(B{chart_data_start_row}:B{chart_data_end_row}, {{"charttype","line";"color","green"}})', "", "", ""])
                data.append(["Whale P&L Trend:", f'=SPARKLINE(C{chart_data_start_row}:C{chart_data_end_row}, {{"charttype","line";"color","blue"}})', "", "", ""])
                data.append(["", "", "", "", ""])

                # Summary statistics for the chart period
                data.append(["Chart Period Summary:", "", "", "", ""])
                data.append([
                    "Data Points",
                    len(pnl_history),
                    "",
                    "",
                    "",
                ])

                # Calculate trends
                if len(our_pnl_values) >= 2:
                    our_change = our_pnl_values[-1] - our_pnl_values[0]
                    whale_change = whale_pnl_values[-1] - whale_pnl_values[0]
                    our_trend = "📈" if our_change > 0 else "📉" if our_change < 0 else "➡️"
                    whale_trend = "📈" if whale_change > 0 else "📉" if whale_change < 0 else "➡️"

                    data.append([
                        "Our Trend",
                        f"{our_trend} {self._format_pnl_percentage(our_change, decimals=2)} over period",
                        "",
                        "",
                        "",
                    ])
                    data.append([
                        "Whale Trend",
                        f"{whale_trend} {self._format_pnl_percentage(whale_change, decimals=2)} over period",
                        "",
                        "",
                        "",
                    ])

                    # Are we converging or diverging from whale?
                    initial_gap = our_pnl_values[0] - whale_pnl_values[0]
                    final_gap = our_pnl_values[-1] - whale_pnl_values[-1]
                    if abs(final_gap) < abs(initial_gap):
                        convergence = "Converging (closing the gap)"
                    elif abs(final_gap) > abs(initial_gap):
                        convergence = "Diverging (gap widening)"
                    else:
                        convergence = "Stable (gap unchanged)"

                    data.append([
                        "Tracking Status",
                        convergence,
                        "",
                        "",
                        "",
                    ])
        else:
            data.append(["Insufficient data for chart", "", "", "", ""])
            data.append(["(Need at least 2 data points, collected every sync cycle)", "", "", "", ""])
            data.append(["", "", "", "", ""])

        # Clear and update in one batch (USER_ENTERED to parse HYPERLINK formulas and SPARKLINE)
        worksheet.clear()
        worksheet.update(range_name="A1", values=data, value_input_option="USER_ENTERED")
        logger.debug("Synced comparison data to Google Sheets")

    def sync_execution_diagnostics(self, diagnostics: List[Dict[str, Any]]) -> None:
        """Sync execution latency/slippage diagnostics to Google Sheets."""
        sheet = self._get_sheet()
        worksheet = sheet.worksheet(TAB_EXECUTION_DIAGNOSTICS)
        headers = [
            "Run ID",
            "Order ID",
            "Market",
            "Side",
            "Order Type",
            "Whale Signal TS",
            "Order Sent TS",
            "Fill TS",
            "Latency (ms)",
            "Best Bid",
            "Best Ask",
            "Mid Price",
            "Fill Price",
            "Spread %",
            "Quote Slippage %",
            "Baseline Slippage %",
            "Half Spread %",
            "Spread Crossed",
            "Whale Ref Type",
            "Whale Ref Price",
        ]
        data = [headers]
        for row in diagnostics:
            data.append([
                row.get("run_id", ""),
                row.get("order_id", ""),
                row.get("market_id", row.get("market", "")),
                row.get("side", row.get("action", "")),
                row.get("order_type", ""),
                row.get("whale_signal_ts", row.get("whale_timestamp", "")),
                row.get("order_sent_ts", row.get("our_timestamp", "")),
                row.get("fill_ts", ""),
                row.get("latency_ms", ""),
                row.get("best_bid", ""),
                row.get("best_ask", ""),
                row.get("mid_price", ""),
                row.get("fill_price", row.get("actual_fill_price", "")),
                row.get("spread_pct", ""),
                row.get("quote_slippage_pct", row.get("slippage_pct", "")),
                row.get("baseline_slippage_pct", ""),
                row.get("half_spread_pct", ""),
                row.get("spread_crossed", ""),
                row.get("whale_ref_type", ""),
                row.get("whale_entry_ref_price", ""),
            ])
        worksheet.clear()
        worksheet.update(range_name="A1", values=data, value_input_option="USER_ENTERED")

    def sync_all(
        self,
        config: Dict[str, Any],
        portfolio_stats: Dict[str, Any],
        target_positions: List[Any],
        our_trades: List[Dict[str, Any]],
        trade_stats: Optional[Dict[str, Any]] = None,
        unrealized_pnl: Optional[float] = None,
        pnl_history: Optional[List[Dict[str, Any]]] = None,
        execution_diagnostics: Optional[List[Dict[str, Any]]] = None,
    ) -> bool:
        """Sync all data to Google Sheets.

        Args:
            config: Copy trader configuration
            portfolio_stats: Portfolio statistics from database
            target_positions: Target wallet positions (Position dataclass instances or dicts)
            our_trades: Our trade records from database
            trade_stats: Trade statistics from database (optional)
            unrealized_pnl: Unrealized P&L from open positions (calculated separately)
            pnl_history: P&L history snapshots for time-series chart

        Returns:
            True if sync was successful, False otherwise
        """
        # Rate limiting: skip if synced recently
        with self._lock:
            if self._last_sync:
                time_since_sync = (datetime.now() - self._last_sync).total_seconds()
                if time_since_sync < self._min_sync_interval:
                    logger.debug(f"Skipping sync, last sync was {time_since_sync:.0f}s ago")
                    return True

        try:
            # Extract config values
            target_wallet = config.get("target_wallet", "Unknown")
            dry_run = config.get("execution", {}).get("dry_run", True)
            initial_budget = config.get("starting_budget", 0)

            # Build whale profile URL
            whale_profile_url = f"https://polymarket.com/profile/{target_wallet}"

            # Reconcile summary values from the ledger rows for consistency.
            cash_available = float(portfolio_stats.get("cash", initial_budget))
            ledger = reconcile_trade_ledger(
                cash=cash_available,
                trades=our_trades,
                starting_equity=initial_budget,
            )
            current_value = ledger.get("total_value", cash_available)
            pnl_24h = portfolio_stats.get("pnl_24h", 0)
            pnl_total = ledger.get("total_realized", portfolio_stats.get("pnl_total", 0))
            session_started = portfolio_stats.get("session_started")
            if unrealized_pnl is None:
                unrealized_pnl = ledger.get("total_unrealized", 0.0)

            # Sync portfolio summary
            self.sync_portfolio(
                target_wallet=target_wallet,
                dry_run=dry_run,
                initial_budget=initial_budget,
                current_value=current_value,
                cash_available=cash_available,
                pnl_24h=pnl_24h,
                pnl_total=pnl_total,
                whale_profile_url=whale_profile_url,
                session_started=session_started,
                trade_stats=trade_stats,
                unrealized_pnl=unrealized_pnl,
            )

            # Convert Position dataclass instances to dicts if needed
            target_pos_dicts = []
            for pos in target_positions:
                if hasattr(pos, "__dict__"):
                    # It's a dataclass or object with attributes
                    pos_dict = {
                        "market": getattr(pos, "market", ""),  # UUID/condition ID
                        "market_slug": getattr(pos, "market_slug", ""),  # Human-readable name
                        "outcome": getattr(pos, "outcome", ""),
                        "size": getattr(pos, "size", 0),
                        "avg_price": getattr(pos, "avg_price", 0),
                        "current_price": getattr(pos, "current_price", 0),
                        "value": getattr(pos, "value", 0),
                        "pnl": getattr(pos, "pnl", 0),
                    }
                else:
                    pos_dict = pos
                target_pos_dicts.append(pos_dict)

            # Sync target positions
            self.sync_target_positions(target_pos_dicts)

            # Sync our trades (pass target positions for slug lookup)
            self.sync_our_trades(our_trades, target_pos_dicts)

            # Sync comparison analysis
            self.sync_comparison(target_pos_dicts, our_trades, trade_stats, pnl_history)
            if execution_diagnostics:
                self.sync_execution_diagnostics(execution_diagnostics)

            with self._lock:
                self._last_sync = datetime.now()
            logger.info("Successfully synced all data to Google Sheets")
            return True

        except Exception as e:
            logger.error(f"Failed to sync to Google Sheets: {e}")
            return False

    def close(self) -> None:
        """Close the gspread client and cleanup resources."""
        with self._lock:
            if self._client:
                self._client = None
                self._sheet = None
                logger.debug("Closed Google Sheets client")


def create_sheets_sync(config: Dict[str, Any]) -> Optional[GoogleSheetsSync]:
    """Create a GoogleSheetsSync instance from config if enabled.

    Args:
        config: Copy trader configuration

    Returns:
        GoogleSheetsSync instance if enabled and configured, None otherwise
    """
    # Check if sheets sync is enabled
    sheets_config = config.get("sheets", {})
    enabled = sheets_config.get("enabled", False)

    if not enabled:
        logger.debug("Google Sheets sync is disabled")
        return None

    sheet_id = sheets_config.get("sheet_id", "")
    credentials_path = sheets_config.get("credentials_path", "")

    if not sheet_id:
        logger.warning("Google Sheets sync enabled but sheet_id not configured")
        return None

    if not credentials_path:
        logger.warning("Google Sheets sync enabled but credentials_path not configured")
        return None

    # Validate credentials file exists and is readable
    if not os.path.isfile(credentials_path):
        logger.warning(f"Credentials path is not a file: {credentials_path}")
        return None

    if not os.access(credentials_path, os.R_OK):
        logger.warning(f"Cannot read credentials file: {credentials_path}")
        return None

    # Validate it's a valid service account JSON
    try:
        with open(credentials_path, "r") as f:
            creds_data = json.load(f)
            if creds_data.get("type") != "service_account":
                logger.warning("Credentials file is not a valid service account JSON")
                return None
    except json.JSONDecodeError:
        logger.warning("Credentials file is not valid JSON")
        return None
    except IOError as e:
        logger.warning(f"Error reading credentials file: {e}")
        return None

    try:
        sync = GoogleSheetsSync(sheet_id=sheet_id, credentials_path=credentials_path)
        # Test connection by getting the sheet
        sync._get_sheet()
        # Log truncated sheet ID for security
        logger.info(f"Google Sheets sync initialized for sheet: {sheet_id[:8]}...")
        return sync
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets sync: {e}")
        return None
