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
    lot_size: float
    entry_price: float
    exit_price: Optional[float] = None
    sl: Optional[float] = None
    status: str = "open"
    open_time: Optional[float] = None
    close_time: Optional[float] = None
    counter_trade_id: Optional[str] = None
    error: Optional[str] = None
    reason: Optional[str] = None