from dataclasses import dataclass
from typing import Optional

@dataclass
class Trade:
    arb_id: str
    ticket: Optional[int]
    asset: str
    broker: str
    counter_party: str
    side: str
    allowed_slip: float
    lot_size: float
    entry_price: float
    exit_price: Optional[float] = None
    sl: Optional[float] = None
    status: str = "pending"
    open_time: Optional[float] = None
    close_time: Optional[float] = None
    pnl: Optional[float] = None
    error: Optional[str] = None