"""
Market trading window schedules for each supported asset.

This module maps asset symbols (e.g. "BTCUSD") to their trading hours,
organized by weekday. Used to prevent order placement outside broker-
permitted windows and avoid unnecessary errors. All times are UTC.
"""

from datetime import datetime, time, UTC
from typing import Dict, List, Tuple

# ASSET_SCHEDULES maps asset symbols to a dict of weekdays (0=Mon) with allowed time intervals.
ASSET_SCHEDULES: Dict[str, Dict[int, List[Tuple[time, time]]]] = {
    "BTCUSD": {
        0: [(time(0, 0), time(20, 59)), (time(21, 30), time(23, 59))],
        1: [(time(0, 0), time(20, 59)), (time(21, 30), time(23, 59))],
        2: [(time(0, 0), time(20, 59)), (time(21, 30), time(23, 59))],
        3: [(time(0, 0), time(20, 59)), (time(21, 30), time(23, 59))],
        4: [(time(0, 0), time(20, 59)), (time(21, 30), time(23, 59))],
        5: [(time(0, 0), time(20, 59)), (time(21, 30), time(23, 59))],
        6: [(time(0, 0), time(20, 59)), (time(21, 30), time(23, 59))],
    },

    "US30": {
        0: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        1: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        2: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        3: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        4: [(time(0, 0), time(20, 59))],
        5: [],
        6: [(time(22, 5), time(23, 59))],
    },

    "US100": {
        0: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        1: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        2: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        3: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        4: [(time(0, 0), time(20, 59))],
        5: [],
        6: [(time(22, 5), time(23, 59))],
    },

    "GER40": {
        0: [(time(0, 20), time(19, 59))],
        1: [(time(0, 20), time(19, 59))],
        2: [(time(0, 20), time(19, 59))],
        3: [(time(0, 20), time(19, 59))],
        4: [(time(0, 20), time(19, 59))],
        5: [],
        6: [],
    },

    "JP225": {
        0: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        1: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        2: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        3: [(time(0, 0), time(20, 59)), (time(22, 5), time(23, 59))],
        4: [(time(0, 0), time(20, 0))],
        5: [],
        6: [(time(22, 5), time(23, 59))],
    },
}


def is_trading_time(asset: str) -> bool:
    """
    Returns True if the asset is currently in a permitted trading window.

    Args:
        asset: Asset symbol (e.g. "BTCUSD", "US30")

    Returns:
        True if now (UTC) is within a trading window for this asset.

    Raises:
        ValueError: If the asset is not configured.
    """
    now = datetime.now(UTC)
    weekday = now.weekday()
    now_time = now.time()

    if asset not in ASSET_SCHEDULES:
        raise ValueError(f"Unknown asset: {asset}")

    day_schedule = ASSET_SCHEDULES[asset].get(weekday, [])
    return any(start <= now_time <= end for start, end in day_schedule)
