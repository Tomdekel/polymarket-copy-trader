"""Discord webhook notifications for trade alerts."""
import logging
import threading
from datetime import datetime
from enum import Enum
from typing import Optional
from urllib.parse import urlparse
from discord_webhook import DiscordWebhook, DiscordEmbed

logger = logging.getLogger("polymarket_copy_trader")


class WebhookValidationError(ValueError):
    """Raised when webhook URL validation fails."""
    pass


class AlertType(Enum):
    """Types of alerts that can be sent."""

    TRADE = "trade"
    RISK = "risk"
    ERROR = "error"
    INFO = "info"


# Color mapping for Discord embeds (decimal color values)
ALERT_COLORS = {
    AlertType.TRADE: 0x00FF00,  # Green
    AlertType.RISK: 0xFFA500,  # Orange
    AlertType.ERROR: 0xFF0000,  # Red
    AlertType.INFO: 0x0099FF,  # Blue
}

# Allowed webhook domains
ALLOWED_WEBHOOK_DOMAINS = [
    "discord.com",
    "discordapp.com",
]


def validate_webhook_url(url: str) -> str:
    """Validate webhook URL for security.

    Args:
        url: The webhook URL to validate

    Returns:
        The validated URL

    Raises:
        WebhookValidationError: If URL is invalid or not allowed
    """
    if not url:
        raise WebhookValidationError("Webhook URL cannot be empty")

    parsed = urlparse(url)

    # Must use HTTPS
    if parsed.scheme != "https":
        raise WebhookValidationError("Webhook URL must use HTTPS")

    # Must be an allowed domain
    domain = parsed.netloc.lower()
    if not any(domain.endswith(allowed) for allowed in ALLOWED_WEBHOOK_DOMAINS):
        raise WebhookValidationError(
            f"Webhook domain not allowed: {domain}. Must be Discord."
        )

    # Must have /api/webhooks/ in path
    if "/api/webhooks/" not in parsed.path:
        raise WebhookValidationError("Invalid Discord webhook URL format")

    return url


class NotificationService:
    """Service for sending Discord webhook notifications."""

    def __init__(self, webhook_url: Optional[str] = None):
        """Initialize notification service.

        Args:
            webhook_url: Discord webhook URL. If None, notifications are disabled.

        Raises:
            WebhookValidationError: If webhook URL is invalid
        """
        if webhook_url:
            self.webhook_url = validate_webhook_url(webhook_url)
            self.enabled = True
        else:
            self.webhook_url = None
            self.enabled = False

    def _send_embed(
        self,
        title: str,
        description: str,
        alert_type: AlertType,
        fields: Optional[dict] = None,
    ) -> bool:
        """Send a Discord embed notification.

        Returns:
            True if sent successfully, False otherwise.
        """
        if not self.enabled:
            logger.debug("Notifications disabled, skipping webhook")
            return False

        try:
            webhook = DiscordWebhook(url=self.webhook_url)
            embed = DiscordEmbed(
                title=title,
                description=description,
                color=ALERT_COLORS.get(alert_type, 0x808080),
            )
            embed.set_timestamp(datetime.now().isoformat())
            embed.set_footer(text="Polymarket Copy Trader")

            if fields:
                for name, value in fields.items():
                    embed.add_embed_field(
                        name=name, value=str(value), inline=True
                    )

            webhook.add_embed(embed)
            response = webhook.execute()

            if response.status_code in (200, 204):
                return True
            else:
                logger.warning(
                    f"Webhook returned status {response.status_code}"
                )
                return False

        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
            return False

    def send_trade_alert(
        self,
        action: str,
        market: str,
        size: float,
        price: float,
        dry_run: bool = True,
    ) -> bool:
        """Send a trade execution alert.

        Args:
            action: BUY or SELL
            market: Market identifier or slug
            size: Trade size in USD
            price: Execution price
            dry_run: Whether this is a simulated trade
        """
        mode = "🧪 DRY RUN" if dry_run else "🔴 LIVE"
        emoji = "📈" if action.upper() == "BUY" else "📉"

        title = f"{emoji} {action.upper()} - {mode}"
        description = f"Trade executed on **{market}**"

        fields = {
            "Size": f"${size:,.2f}",
            "Price": f"{price:.4f}",
            "Action": action.upper(),
        }

        return self._send_embed(title, description, AlertType.TRADE, fields)

    def send_risk_alert(
        self,
        reason: str,
        current_pnl: float,
        daily_pnl: Optional[float] = None,
    ) -> bool:
        """Send a risk limit alert.

        Args:
            reason: Reason trading was halted
            current_pnl: Current total PnL
            daily_pnl: Current daily PnL (optional)
        """
        title = "⚠️ Risk Limit Triggered"
        description = f"Trading halted: **{reason}**"

        fields = {"Total P&L": f"${current_pnl:,.2f}"}
        if daily_pnl is not None:
            fields["Daily P&L"] = f"${daily_pnl:,.2f}"

        return self._send_embed(title, description, AlertType.RISK, fields)

    def send_error_alert(
        self,
        error_type: str,
        message: str,
        details: Optional[str] = None,
    ) -> bool:
        """Send an error alert.

        Args:
            error_type: Type of error (e.g., "API Error", "Database Error")
            message: Error message
            details: Additional details (optional)
        """
        title = f"❌ {error_type}"
        description = message

        fields = {}
        if details:
            fields["Details"] = details[:1024]  # Discord field limit

        return self._send_embed(title, description, AlertType.ERROR, fields)

    def send_startup_notification(
        self,
        target_wallet: str,
        budget: float,
        dry_run: bool,
    ) -> bool:
        """Send a startup notification.

        Args:
            target_wallet: Target wallet being tracked
            budget: Starting budget
            dry_run: Whether running in simulation mode
        """
        mode = "Simulation" if dry_run else "Live"
        title = "🚀 Copy Trader Started"
        description = f"Now tracking wallet `{target_wallet[:10]}...{target_wallet[-6:]}`"

        fields = {
            "Budget": f"${budget:,.2f}",
            "Mode": mode,
        }

        return self._send_embed(title, description, AlertType.INFO, fields)

    def send_shutdown_notification(
        self,
        reason: str = "Manual shutdown",
        final_pnl: Optional[float] = None,
    ) -> bool:
        """Send a shutdown notification.

        Args:
            reason: Reason for shutdown
            final_pnl: Final PnL value (optional)
        """
        title = "🛑 Copy Trader Stopped"
        description = f"Reason: {reason}"

        fields = {}
        if final_pnl is not None:
            fields["Final P&L"] = f"${final_pnl:,.2f}"

        return self._send_embed(title, description, AlertType.INFO, fields)
