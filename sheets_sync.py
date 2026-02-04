"""Google Sheets sync for Polymarket Copy Trader dashboard."""
import json
import os
import logging
from datetime import datetime
from threading import Lock
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Tab names
TAB_PORTFOLIO = "Portfolio Summary"
TAB_TARGET_POSITIONS = "Target Positions"
TAB_OUR_TRADES = "Our Trades"


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
        for tab_name in [TAB_PORTFOLIO, TAB_TARGET_POSITIONS, TAB_OUR_TRADES]:
            if tab_name not in existing_tabs:
                sheet.add_worksheet(title=tab_name, rows=100, cols=10)
                logger.info(f"Created sheet tab: {tab_name}")

    def _format_currency(self, value: float) -> str:
        """Format value as currency string."""
        if value is None:
            return "$0.00"
        if value >= 0:
            return f"${value:,.2f}"
        return f"-${abs(value):,.2f}"

    def _format_pnl(self, value: float) -> str:
        """Format PnL with + prefix for positive values."""
        if value is None:
            return "$0.00"
        if value >= 0:
            return f"+${value:,.2f}"
        return f"-${abs(value):,.2f}"

    def _format_percentage(self, value: float) -> str:
        """Format value as percentage."""
        return f"{value:.2%}"

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
    ) -> None:
        """Sync portfolio summary to the Portfolio Summary tab.

        Args:
            target_wallet: Target wallet address being copied
            dry_run: Whether running in dry-run mode
            initial_budget: Initial budget amount
            current_value: Current portfolio value
            cash_available: Available cash balance
            pnl_24h: Profit/loss in last 24 hours
            pnl_total: Total profit/loss
            whale_profile_url: URL to whale's Polymarket profile
            session_started: ISO timestamp when session started
            trade_stats: Dictionary of trade statistics from database
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

        # Calculate allocated cash (invested in open positions)
        allocated_in_deals = initial_budget - cash_available
        if allocated_in_deals < 0:
            allocated_in_deals = 0  # Handle edge case

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
            ["P&L (24h)", self._format_pnl(pnl_24h)],
            ["P&L (Total)", self._format_pnl(pnl_total)],
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
                ["Total Buys", stats.get("total_buys", 0)],
                ["Total Sells", stats.get("total_sells", 0)],
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
                - market: Market identifier/slug
                - outcome: YES or NO
                - size: Number of shares
                - avg_price: Average entry price
                - current_price: Current market price
                - value: Current position value
                - pnl: Unrealized P&L
        """
        sheet = self._get_sheet()
        worksheet = sheet.worksheet(TAB_TARGET_POSITIONS)

        # Header row
        headers = ["Market", "Outcome", "Size", "Avg Price", "Current Price", "Value", "P&L"]
        data = [headers]

        # Add position rows
        for pos in positions:
            size = pos.get('size') or 0
            avg_price = pos.get('avg_price') or 0
            current_price = pos.get('current_price') or 0
            value = pos.get('value') or 0
            pnl = pos.get('pnl') or 0

            row = [
                pos.get("market") or pos.get("market_slug") or "Unknown",
                pos.get("outcome") or "",
                f"{size:.4f}",
                f"{avg_price:.4f}",
                f"{current_price:.4f}",
                self._format_currency(value),
                self._format_pnl(pnl),
            ]
            data.append(row)

        # Clear and update in one batch
        worksheet.clear()
        worksheet.update(range_name="A1", values=data)
        logger.debug(f"Synced {len(positions)} target positions to Google Sheets")

    def sync_our_trades(self, trades: List[Dict[str, Any]], max_trades: int = 500) -> None:
        """Sync our trades to the Our Trades tab.

        Args:
            trades: List of our trades with keys:
                - timestamp: Trade timestamp
                - market: Market identifier
                - side: BUY or SELL
                - size: Trade size in USD
                - price: Trade price
                - pnl: Unrealized P&L (optional)
                - status: Trade status (open/closed)
            max_trades: Maximum number of recent trades to sync (default: 500)
        """
        # Limit to most recent trades to avoid memory issues
        trades = trades[:max_trades] if len(trades) > max_trades else trades
        sheet = self._get_sheet()
        worksheet = sheet.worksheet(TAB_OUR_TRADES)

        # Header row
        headers = ["Opened", "Market", "Size", "Buy Price", "Sell Price", "P&L", "Closed", "Status"]
        data = [headers]

        # Add trade rows
        for trade in trades:
            # Format open timestamp
            open_timestamp = trade.get("timestamp", "")
            if isinstance(open_timestamp, str) and "T" in open_timestamp:
                try:
                    dt = datetime.fromisoformat(open_timestamp)
                    open_timestamp = dt.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    pass

            # Format close timestamp
            close_timestamp = trade.get("closed_at", "")
            if isinstance(close_timestamp, str) and "T" in close_timestamp:
                try:
                    dt = datetime.fromisoformat(close_timestamp)
                    close_timestamp = dt.strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    pass
            if not close_timestamp:
                close_timestamp = "-"

            pnl = trade.get("pnl")
            pnl_str = self._format_pnl(pnl) if pnl is not None else "-"

            size = trade.get("size") or 0
            buy_price = trade.get("price") or 0
            sell_price = trade.get("sell_price")
            sell_price_str = f"{sell_price:.4f}" if sell_price is not None else "-"

            row = [
                open_timestamp,
                trade.get("market") or "Unknown",
                self._format_currency(size),
                f"{buy_price:.4f}",
                sell_price_str,
                pnl_str,
                close_timestamp,
                trade.get("status") or "open",
            ]
            data.append(row)

        # Clear and update in one batch
        worksheet.clear()
        worksheet.update(range_name="A1", values=data)
        logger.debug(f"Synced {len(trades)} trades to Google Sheets")

    def sync_all(
        self,
        config: Dict[str, Any],
        portfolio_stats: Dict[str, Any],
        target_positions: List[Any],
        our_trades: List[Dict[str, Any]],
        trade_stats: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Sync all data to Google Sheets.

        Args:
            config: Copy trader configuration
            portfolio_stats: Portfolio statistics from database
            target_positions: Target wallet positions (Position dataclass instances or dicts)
            our_trades: Our trade records from database
            trade_stats: Trade statistics from database (optional)

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

            # Extract portfolio values
            current_value = portfolio_stats.get("total_value", initial_budget)
            cash_available = portfolio_stats.get("cash", initial_budget)
            pnl_24h = portfolio_stats.get("pnl_24h", 0)
            pnl_total = portfolio_stats.get("pnl_total", 0)
            session_started = portfolio_stats.get("session_started")

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
            )

            # Convert Position dataclass instances to dicts if needed
            target_pos_dicts = []
            for pos in target_positions:
                if hasattr(pos, "__dict__"):
                    # It's a dataclass or object with attributes
                    pos_dict = {
                        "market": getattr(pos, "market_slug", None) or getattr(pos, "market", "Unknown"),
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

            # Sync our trades
            self.sync_our_trades(our_trades)

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
