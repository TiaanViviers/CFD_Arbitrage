"""
Loss Injection Module logic for CFD Arbitrage system.

The goal and purpose of this module is to "disguise" our arbitrage activities
and appear more like normal day-traders.

Handles opening, managing, and syncing LIM trades across supported brokers.
Functions are called by the master process as part of the arbitrage engine.
"""

import random
import uuid
import hashlib
import math
import logging
from datetime import datetime, UTC

from trade import Trade

############################### Config & Globals ###############################

logger = logging.getLogger("arbitrage_bot")
MAX_WINRATE = 0.65
BROKERS = ["icmarkets", "exness", "fxtm", "eightcap", "xm"]


################################ LIM Open Logic ################################
def open_lim(open_lim: list[Trade], closed_lim: list[Trade], closed_trades: list,
             asset_conf: dict, balances_df, price_matrix,
             worker_cmd_queues, worker_resp_queues, telebot
             ) -> list[Trade]:
    """
    Try to open a LIM trade for each broker not currently holding one.
    """
    for broker in BROKERS:
        if _has_lim(broker, open_lim):
            continue
        winrate = _get_winrate(broker, closed_trades, closed_lim)
        if winrate > MAX_WINRATE:
            lim_trade = _init_lim_trade(broker, asset_conf, balances_df, price_matrix)
            lim_trade = _place_lim_trade(lim_trade, worker_cmd_queues, worker_resp_queues)
            if lim_trade.status == "open":
                logger.info(f"Opened LIM Trade on {lim_trade.broker}")
                telebot.open_lim(lim_trade, winrate)
                open_lim.append(lim_trade)
    return open_lim


def _has_lim(broker_name: str, open_lim: list[Trade]) -> bool:
    """Return True if a LIM trade is already open with this broker."""
    return any(tr.broker == broker_name for tr in open_lim)


################################ Win rate Logic ################################
def _get_winrate(broker_name: str, closed_trades: list[tuple[Trade, Trade]],
                closed_lim: list[Trade]) -> float:
    """
    Calculates the win rate for a specific broker across all trades.
    Returns 0 if <5 trades.
    """
    trades = [
        tr
        for pair in closed_trades
        for tr in pair
        if tr.broker == broker_name
    ] + [
        tr
        for tr in closed_lim
        if tr.broker == broker_name and tr.status == "closed"
    ]

    total = len(trades)
    if total < 5:
        return 0.0
    wins = sum(1 for tr in trades if tr.pnl > 0)
    return wins / total


################################ LIM Trade Construction ################################
def _init_lim_trade(broker_name: str, asset_conf: dict, balances_df, price_matrix) -> Trade:
    """
    Create a new LIM Trade object with randomized parameters.
    """
    u = uuid.uuid4()
    h = hashlib.sha256(u.bytes).digest()
    arb_id = int.from_bytes(h[:4], 'big') & 0x7FFFFFFF

    side = random.choice(["sell", "buy"])
    entry_price = (
        price_matrix.loc[broker_name, "bid"] if side == "sell"
        else price_matrix.loc[broker_name, "ask"]
    )
    balance = balances_df[broker_name]
    lot = _calculate_lot_size(entry_price, balance, asset_conf)
    sl, tp = _calculate_sl_tp(entry_price, side, asset_conf)

    return Trade(
        arb_id=arb_id,
        broker=broker_name,
        counter_party="LIM",
        side=side,
        allowed_slip=asset_conf["allowed_slip"],
        entry_price=entry_price,
        lot_size=lot,
        sl=sl,
        tp=tp,
    )


def _calculate_lot_size(entry_price, balance, asset_conf) -> float:
    """
    Calculate lot size for risk control on LIM trade.
    Ensures lot size is a valid multiple of min_lot (by flooring).
    """
    target_loss_percent = 0.003
    sl_pct = asset_conf.get("lim_sl_pct", 0.003)
    target_loss_usd = balance * target_loss_percent
    sl_distance = entry_price * sl_pct
    min_lot = asset_conf["min_lot"]
    if sl_distance == 0:
        return min_lot

  
    lot_size_raw = target_loss_usd / sl_distance

    # Determine the allowed precision
    min_lot_str = str(min_lot)
    if '.' in min_lot_str:
        decimals = len(min_lot_str.split('.')[1])
    else:
        decimals = 0

    # Floor to nearest allowed increment
    factor = 10 ** decimals
    lot_size_floored = math.floor(lot_size_raw * factor) / factor

    return max(min_lot, lot_size_floored)


def _calculate_sl_tp(entry_price, side, asset_conf):
    """
    Randomize SL/TP for LIM trade.
    """
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


############################# LIM Trade Placement ##############################
def _place_lim_trade(trade: Trade, worker_cmd_queues, worker_resp_queues) -> Trade:
    """
    Place a LIM trade using worker process, update status from response.
    """
    worker_cmd_queues[trade.broker].put({"action": "open_trade", "trade": trade})
    trade_resp = worker_resp_queues[trade.broker].get()
    if trade_resp.status == "open":
        logger.info(
            f"LIM trade opened on {trade.broker} @ {trade.lot_size} lots"
        )
    else:
        logger.info(f"LIM trade failed on {trade.broker}.")
    return trade_resp


################################ LIM Sync Logic ################################
def sync_lim_trades(closed_lim: list[Trade], open_lim: list[Trade],
                    broker_positions, price_matrix, telebot):
    """
    Scan all open LIM trades for closure, finalize and notify as needed.
    """
    still_open, just_closed = _find_closed_lim_trades(open_lim, broker_positions)
    for tr in just_closed:
        _finalize_lim_trade(tr, price_matrix, telebot)
    closed_lim.extend(just_closed)
    return closed_lim, still_open


def _find_closed_lim_trades(open_lim, broker_positions):
    """
    Identify LIM trades that have been closed out on the broker.
    """
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


def _finalize_lim_trade(tr, price_matrix, telebot):
    """
    Complete and close a LIM trade, send notification.
    """
    # Exit price from latest tick
    try:
        tick = price_matrix.loc[tr.broker]
        tr.exit_price = tick["bid"] if tr.side == "buy" else tick["ask"]
    except KeyError:
        tr.exit_price = tr.entry_price

    tr.close_time = datetime.now(UTC).isoformat()
    tr.status = "closed"
    tr.pnl = (
        (tr.exit_price - tr.entry_price) if tr.side == "buy"
        else (tr.entry_price - tr.exit_price)
    ) * tr.lot_size

    telebot.close_lim(tr)
    return tr
