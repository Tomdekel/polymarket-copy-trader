"""Risk management and position limits."""
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

class RiskManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config.get("risk_management", {})
        self.filters = config.get("filters", {})
        self.max_daily_loss = self.config.get("max_daily_loss_pct", 0.10)
        self.max_total_loss = self.config.get("max_total_loss_pct", 0.25)
        self.cooldown_seconds = self.config.get("cooldown_after_loss", 300)
        self.min_liquidity = self.filters.get("min_liquidity", 1000)
        self.max_slippage = self.filters.get("max_slippage", 0.05)
        self.starting_budget: Optional[float] = None
        self.last_loss_time: Optional[datetime] = None
        self.daily_pnl: float = 0
        self.total_pnl: float = 0
        self.last_reset: datetime = datetime.now()
    
    def set_starting_budget(self, budget: float) -> None:
        self.starting_budget = budget
    
    def check_risk(self, current_pnl: float) -> Dict[str, Any]:
        """Check if trading should be halted due to risk limits."""
        self.total_pnl = current_pnl
        
        # Reset daily P&L at midnight
        if datetime.now() - self.last_reset > timedelta(days=1):
            self.daily_pnl = 0
            self.last_reset = datetime.now()
        
        result = {"allow_trade": True, "reason": None}
        
        if self.starting_budget is None:
            return result
        
        # Check daily loss limit
        daily_loss_pct = abs(min(0, self.daily_pnl)) / self.starting_budget
        if daily_loss_pct >= self.max_daily_loss:
            result["allow_trade"] = False
            result["reason"] = f"Daily loss limit hit: {daily_loss_pct:.1%}"
            
        # Check total loss limit
        total_loss_pct = abs(min(0, self.total_pnl)) / self.starting_budget
        if total_loss_pct >= self.max_total_loss:
            result["allow_trade"] = False
            result["reason"] = f"Total loss limit hit: {total_loss_pct:.1%}"
        
        # Check cooldown after big loss
        if self.last_loss_time:
            elapsed = (datetime.now() - self.last_loss_time).total_seconds()
            if elapsed < self.cooldown_seconds:
                result["allow_trade"] = False
                result["reason"] = f"Cooldown: {int(self.cooldown_seconds - elapsed)}s remaining"
        
        return result
    
    def record_loss(self) -> None:
        self.last_loss_time = datetime.now()
    
    def can_trade_market(self, market_liquidity: float, market_slug: str) -> bool:
        """Check if market meets liquidity requirements."""
        if market_liquidity < self.min_liquidity:
            return False
        return True
