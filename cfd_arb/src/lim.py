import random
import uuid
import hashlib
import logging
from datetime import datetime, UTC

from trade import Trade

logger = logging.getLogger("arbitrage_bot")
MAX_WINRATE = 0.65
BROKERS = ["icmarkets", "exness", "fxtm", "eightcap", "xm"]

def open_lim(open_lim, closed_lim, closed_trades, asset_conf, balances_df, price_matrix,
            worker_cmd_queues, worker_resp_queues):
    for broker in BROKERS:
        if has_lim(broker, open_lim):
            continue
        winrate = get_winrate(broker, closed_trades, closed_lim)
        if winrate > MAX_WINRATE:
            lim_trade = init_lim_trade(broker, asset_conf, balances_df, price_matrix)
            lim_trade = place_lim_trade(lim_trade, worker_cmd_queues, worker_resp_queues)
            if lim_trade.status == "open":
                logger
                open_lim.append(lim_trade)
    
    return open_lim


def has_lim(broker_name, open_lim):
    for tr in open_lim:
        if tr.broker == broker_name:
            return True
    
    return False


def get_winrate(broker_name, closed_trades, closed_lim):
    """
    Calculates the win rate for a specific broker.

    Args:
        broker_name (str): Broker to evaluate.
        closed_trades (list[tuple]): List of (sell, buy) Trade tuples.
        closed_lim (list): List of LIM Trade objects (flat list).

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
    for tr in closed_lim:
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
    tp_pct = random.uniform(2.5, 4.2)

    sl_distance = entry_price * sl_pct
    tp_distance = sl_distance * tp_pct

    if side == "buy":
        sl = entry_price - sl_distance
        tp = entry_price + tp_distance
    else:
        sl = entry_price + sl_distance
        tp = entry_price - tp_distance

    return sl, tp


def place_lim_trade(trade, worker_cmd_queues, worker_resp_queues):
    # Send open trade commands
    worker_cmd_queues[trade.broker].put({"action": "open_trade", "trade": trade})
    # Get response
    trade_resp = worker_resp_queues[trade.broker].get()
    # Check responses
    if trade_resp.status == "open":
        logger.info(f"LIM trade opened successfully on {trade.broker} @ {trade.lot_size} lots")
    else:
        logger.info(f"LIM trade failed on {trade.broker}..")
    
    return trade


def sync_lim_trades(closed_lim, open_lim, broker_positions, price_matrix):
    still_open, just_closed = find_closed_lim_trades(open_lim, broker_positions)

    for tr in just_closed:
        finalize_lim_trade(tr, price_matrix)
    closed_lim.extend(just_closed)

    return closed_lim, still_open


def find_closed_lim_trades(open_lim, broker_positions):
    # build live-id sets per broker
    live_ids = {
      b: {pos["magic"] for pos in ps}
      for b, ps in broker_positions.items()
    }

    still_open, just_closed = [], []
    for tr in open_lim:
        if tr.arb_id in live_ids.get(tr.broker, set()):
            still_open.append(tr)
        else:
            just_closed.append(tr)
    return still_open, just_closed


def finalize_lim_trade(tr, price_matrix):
    # exit price from latest tick
    try:
        tick = price_matrix.loc[tr.broker]
        tr.exit_price = tick["bid"] if tr.side == "buy" else tick["ask"]
    except KeyError:
        tr.exit_price = tr.entry_price

    tr.close_time = datetime.now(UTC).isoformat()
    tr.status = "closed"

    # compute pnl
    tr.pnl = ((tr.exit_price - tr.entry_price) if tr.side == "buy"
              else (tr.entry_price - tr.exit_price)) * tr.lot_size
    
    return tr
