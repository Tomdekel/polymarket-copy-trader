"""Position sizing calculations."""
from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class SizedPosition:
    market: str
    action: str  # BUY, SELL, or HOLD
    target_size: float
    our_size: float
    target_percentage: float
    our_percentage: float

class PositionSizer:
    def __init__(self, budget: float, config: Dict[str, Any]):
        self.budget = budget
        self.config = config.get("position_sizing", {})
        self.max_pct = self.config.get("max_position_pct", 0.15)
        self.min_pct = self.config.get("min_position_pct", 0.01)
    
    def calculate_positions(self, target_portfolio_value: float, 
                           target_positions: List[Dict[str, Any]],
                           our_current_positions: List[Dict[str, Any]]) -> List[SizedPosition]:
        """Calculate proportional positions based on target wallet allocation."""
        sized = []
        our_positions_map = {p["market"]: p for p in our_current_positions}
        
        for target_pos in target_positions:
            market = target_pos["market"]
            target_pct = target_pos.get("value", 0) / target_portfolio_value if target_portfolio_value > 0 else 0
            
            # Calculate our target position size (proportional to our budget)
            our_target_size = self.budget * target_pct
            
            # Apply limits
            our_target_size = min(our_target_size, self.budget * self.max_pct)
            if our_target_size < self.budget * self.min_pct:
                our_target_size = 0  # Skip if too small
            
            # Check current position
            our_current = our_positions_map.get(market, {})
            our_current_size = our_current.get("size", 0)
            
            # Determine action
            if our_current_size == 0 and our_target_size > 0:
                action = "BUY"
                size = our_target_size
            elif our_current_size > 0 and our_target_size == 0:
                action = "SELL"
                size = our_current_size
            elif abs(our_target_size - our_current_size) / max(our_current_size, 1) > 0.1:
                # Rebalance if >10% difference
                action = "BUY" if our_target_size > our_current_size else "SELL"
                size = abs(our_target_size - our_current_size)
            else:
                action = "HOLD"
                size = 0
            
            our_pct = (our_current_size / self.budget) if self.budget > 0 else 0
            
            sized.append(SizedPosition(
                market=market,
                action=action,
                target_size=target_pos.get("size", 0),
                our_size=size,
                target_percentage=target_pct,
                our_percentage=our_pct
            ))
        
        return sized
