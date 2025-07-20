from datetime import datetime, time, UTC


ASSET_SCHEDULES = {
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
        0: [(time(0, 0),  time(20, 59)),  (time(22, 0), time(23, 59))],
        1: [(time(0, 0),  time(20, 59)),  (time(22, 0), time(23, 59))],
        2: [(time(0, 0),  time(20, 59)),  (time(22, 0), time(23, 59))],
        3: [(time(0, 0),  time(20, 59)),  (time(22, 0), time(23, 59))],
        4: [(time(0, 0),  time(20, 59))],
        5: [],
        6: [(time(22, 0), time(23, 59))],
    },

    "US100": {
        0: [(time(0, 0),  time(20, 59)),  (time(22, 0), time(23, 59))],
        1: [(time(0, 0),  time(20, 59)),  (time(22, 0), time(23, 59))],
        2: [(time(0, 0),  time(20, 59)),  (time(22, 0), time(23, 59))],
        3: [(time(0, 0),  time(20, 59)),  (time(22, 0), time(23, 59))],
        4: [(time(0, 0),  time(20, 59))],
        5: [],
        6: [(time(22, 0), time(23, 59))],
    },

    "GER40": {
        0: [(time(0, 15), time(19, 59))],
        1: [(time(0, 15), time(19, 59))],
        2: [(time(0, 15), time(19, 59))],
        3: [(time(0, 15), time(19, 59))],
        4: [(time(0, 15), time(19, 59))],
        5: [],
        6: [],
    },

    "JP225": {
        0: [(time(0, 0),  time(20, 59)), (time(22, 0), time(23, 59))],
        1: [(time(0, 0),  time(20, 59)), (time(22, 0), time(23, 59))],
        2: [(time(0, 0),  time(20, 59)), (time(22, 0), time(23, 59))],
        3: [(time(0, 0),  time(20, 59)), (time(22, 0), time(23, 59))],
        4: [(time(0, 0),  time(20, 0))],
        5: [],
        6: [(time(22, 0), time(23, 59))],
    }
}


def is_trading_time(asset):
    """
    Returns True if the asset is currently in a permitted trading window, else False.
    """
    now = datetime.now(UTC)
    weekday = now.weekday()
    now_time = now.time()
    
    if asset not in ASSET_SCHEDULES:
        raise ValueError(f"Unknown asset: {asset}")
    
    day_schedule = ASSET_SCHEDULES[asset].get(weekday, [])
    for start, end in day_schedule:
        if start <= now_time <= end:
            return True
    return False