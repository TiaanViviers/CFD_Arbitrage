from dataclasses import dataclass
from typing import Optional

@dataclass
class Trade:
    arb_id: int
    broker: str
    counter_party: str
    side: str
    allowed_slip: float
    lot_size: float
    entry_price: float
    sl: float
    ticket: Optional[int] = None
    asset: Optional[str] = None
    exit_price: Optional[float] = None
    status: str = "pending"
    open_time: Optional[str] = None
    close_time: Optional[str] = None
    pnl: Optional[float] = None
    error: Optional[str] = None