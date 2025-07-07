import random
import uuid
import hashlib

from trade import Trade

MAX_WINRATE = 0.65
BROKERS = ["icmarkets", "exness", "fxtm", "eightcap", "xm"]

def open_lim(lim_open, lim_closed, closed_trades, asset_conf, balances_df, price_matrix,
            worker_cmd_queues, worker_resp_queues):
    for broker in BROKERS:
        if has_lim(broker, lim_open):
            continue
        winrate = get_winrate(broker, lim_closed, closed_trades)
        if winrate > MAX_WINRATE:
            lim_trade = init_lim_trade(broker, asset_conf, balances_df, price_matrix)
            lim_trade = place_lim_trade(lim_trade, broker, worker_cmd_queues, worker_resp_queues)
            if lim_trade.status == "open":
                lim_open.append(lim_trade)
    
    return lim_open


def has_lim(broker_name, lim_open):
    for tr in lim_open:
        if tr.broker == broker_name:
            return True
    
    return False


def get_winrate(broker_name, closed_trades, lim_closed):
    """
    Calculates the win rate for a specific broker.

    Args:
        broker_name (str): Broker to evaluate.
        closed_trades (list[tuple]): List of (sell, buy) Trade tuples.
        lim_closed (list): List of LIM Trade objects (flat list).

    Returns:
        float: Win rate if >= 5 trades, else 0.
    """
    total = 0
    wins = 0

    # Loop through normal trade tuples
    for sell_tr, buy_tr in closed_trades:
        for tr in (sell_tr, buy_tr):
            if tr.broker != broker_name:
                continue
            total += 1
            if tr.pnl > 0:
                wins += 1

    # Loop through LIM trade list
    for tr in lim_closed:
        if tr.broker != broker_name or tr.status != "closed":
            continue
        total += 1
        if tr.pnl > 0:
            wins += 1

    if total < 5:
        return 0
    
    return wins / total


def init_lim_trade(broker_name, asset_conf, balances_df, price_matrix):
    u = uuid.uuid4()
    h = hashlib.sha256(u.bytes).digest()
    arb_id = int.from_bytes(h[:4], 'big') & 0x7FFFFFFF

    side = random.choice(["sell", "buy"])
    if side == "sell":
        entry_price = price_matrix.loc[broker_name, "bid"]
    elif side == "buy":
        entry_price = price_matrix.loc[broker_name, "ask"]

    balance = balances_df[broker_name]
    lot = calculate_lot_size(entry_price, balance, asset_conf)
    sl, tp = calculate_sl_tp(entry_price, side, asset_conf)

    return Trade(
        arb_id=arb_id,
        broker=broker_name,
        counter_party="LIM",
        side=side,
        allowed_slip=asset_conf["allowed_slip"],
        entry_price=entry_price,
        lot_size=lot,
        sl=sl,
        tp=tp
    )


def calculate_lot_size(entry_price, balance, asset_conf):
    target_loss_percent = 0.003             #0.3%
    sl_pct = asset_conf.get("lim_sl_pct", 0.003)

    target_loss_usd = balance * target_loss_percent
    sl_distance = entry_price * sl_pct

    if sl_distance == 0:
        return asset_conf["min_lot"]

    lot_size = round(target_loss_usd / sl_distance, 2)
    return max(asset_conf["min_lot"], lot_size)


def calculate_sl_tp(entry_price, side, asset_conf):
    sl_pct = asset_conf.get("lim_sl_pct", 0.003)
    tp_pct = 3

    sl_distance = entry_price * sl_pct
    tp_distance = sl_distance * tp_pct

    if side == "buy":
        sl = entry_price - sl_distance
        tp = entry_price + tp_distance
    else:
        sl = entry_price + sl_distance
        tp = entry_price - tp_distance

    return sl, tp