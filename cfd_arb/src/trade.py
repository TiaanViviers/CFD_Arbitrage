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
    sl: Optional[float] = None
    tp: Optional[float] = None
    ticket: Optional[int] = None
    asset: Optional[str] = None
    exit_price: Optional[float] = None
    status: str = "pending"
    open_time: Optional[str] = None
    close_time: Optional[str] = None
    pnl: Optional[float] = 0
    error: Optional[str] = None