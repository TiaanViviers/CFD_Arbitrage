from dataclasses import dataclass
from typing import Optional

@dataclass
class Trade:
    """Single trade event/leg in the arbitrage system."""
    arb_id: int
    broker: str
    counter_party: str
    side: str
    allowed_slip: float
    lot_size: float
    entry_price: float
    status: str = "pending"
    sl: Optional[float] = None
    tp: Optional[float] = None
    ticket: Optional[int] = None
    asset: Optional[str] = None
    exit_price: Optional[float] = None
    open_time: Optional[str] = None
    close_time: Optional[str] = None
    pnl: Optional[float] = 0
    error: Optional[str] = None