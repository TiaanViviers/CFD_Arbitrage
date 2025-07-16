from datetime import datetime, UTC
import time
import pandas as pd
import numpy as np
import math
import uuid
import hashlib
import random
import logging

from telebot import TeleBot
from trade import Trade
from lim import open_lim, sync_lim_trades

logger = logging.getLogger("arbitrage_bot")
telebot = TeleBot()

LOCKFILE = "../locks/arb_lock_file.lock"

def master_proc(asset, asset_config, worker_cmd_queues, worker_resp_queues):
    open_trades       = [] 
    closed_trades     = []
    open_lim_trades   = [] 
    closed_lim_trades = []
    telebot.set_asset(asset)

    try:
        while True:
            #Data collection
            request_worker_ticks(worker_cmd_queues)
            price_matrix, balances_df = get_worker_ticks(worker_resp_queues)

            # Open new trades
            open_trades = open_available_trades(asset_config, price_matrix, balances_df,
                                                open_trades, worker_cmd_queues, worker_resp_queues)
            open_lim_trades = open_lim(open_lim_trades, closed_lim_trades, closed_trades,
                                       asset_config, balances_df, price_matrix,
                                       worker_cmd_queues, worker_resp_queues, telebot)

            # Close eligible trades
            closed_trades, open_trades = close_available_trades(open_trades, closed_trades, price_matrix,
                                                                worker_cmd_queues, worker_resp_queues)

            # Sync & clean up
            broker_positions = request_broker_positions(worker_cmd_queues, worker_resp_queues)
            closed_trades, open_trades = sync_arb_trades(closed_trades, open_trades, broker_positions)
            closed_lim_trades, open_lim_trades = sync_lim_trades(closed_lim_trades, open_lim_trades,
                                                                 broker_positions, price_matrix, telebot)
            clean_rogue_trades(open_trades, open_lim_trades, broker_positions, worker_cmd_queues, 
                               worker_resp_queues)
            

            # Daily telegram update
            daily_update(balances_df, telebot)
            
            time.sleep(0.2)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received — shutting down workers...")
        for broker, cmd_q in worker_cmd_queues.items():
            cmd_q.put({"action": "shutdown"})
        logger.info("Shutdown commands sent. Exiting master_proc.")


def request_worker_ticks(worker_cmd_queues):
    for q in worker_cmd_queues.values():
        q.put({"action": "get_tick"})


def get_worker_ticks(worker_resp_queues):
    ticks = {}
    balances = {}
    for broker, resp_q in worker_resp_queues.items():
        resp = resp_q.get()
        ticks[broker] = resp.get("tick")
        balances[broker] = resp.get("balance")

    price_matrix = build_price_matrix(ticks)
    balances_df  = pd.Series(balances, name="balance").sort_index()
    return price_matrix, balances_df


def daily_update(balances, telebot):
    now = datetime.now(UTC)
    if now.hour == 21 and now.minute == 3:
        if daily_update_guard(now.strftime("%Y-%m-%d")):
            telebot.daily_report(balances)

def daily_update_guard(today_str):
    """Return True if report should be sent; else False."""
    with open(LOCKFILE, "r") as f:
        if f.read().strip() == today_str:
            return False
    with open(LOCKFILE, "w") as f:
        f.write(today_str)
    return True



def build_price_matrix(ticks):
    """
    Convert a dict of ticks to a pandas DataFrame price matrix.
    Index: broker name
    Columns: ['bid', 'ask']
    """
    # Defensive: filter out brokers with None tick
    clean_ticks = {broker: tick for broker, tick in ticks.items() if tick is not None}
    if not clean_ticks:
        return pd.DataFrame(columns=["bid", "ask"])

    df = pd.DataFrame.from_dict(clean_ticks, orient="index")
    df = df[["bid", "ask"]]
    df = df.sort_index()
    return df


def open_available_trades(asset_config, price_matrix, balances_df, open_trades,
                          worker_cmd_queues, worker_resp_queues):

    candidates = find_divergent_pairs(price_matrix, asset_config["entry_threshold"])
    if not candidates: return open_trades

    for sell_broker, buy_broker, div in candidates:
        if can_open_trade(open_trades, sell_broker, buy_broker):
            sell_lot = calculate_lots(sell_broker, balances_df, asset_config, div)
            buy_lot = calculate_lots(buy_broker, balances_df, asset_config, div)
            lot_size = min(sell_lot, buy_lot)
            if lot_size < asset_config["min_lot"]:
                continue

            sell_trade = init_trade(sell_broker, buy_broker, "sell", lot_size,
                                    price_matrix, asset_config["allowed_slip"])
            arb_id = sell_trade.arb_id
            buy_trade = init_trade(buy_broker, sell_broker, "buy", lot_size,
                                price_matrix, asset_config["allowed_slip"], arb_id=arb_id)

            trade_pair = place_trade_pair(sell_trade, buy_trade, worker_cmd_queues, worker_resp_queues)
            open_trades.append(trade_pair)

    return open_trades
               

def find_divergent_pairs(price_matrix, threshold):
    """
    For every broker pair, computes divergence = bid[sell_broker] - ask[buy_broker].
    Returns a list of (sell_broker, buy_broker, divergence) for pairs where divergence >= threshold.
    """
    qualifying = []
    brokers = price_matrix.index
    for sell_broker in brokers:
        sell_bid = price_matrix.at[sell_broker, 'bid']
        if sell_bid is None or np.isnan(sell_bid): continue
        for buy_broker in brokers:
            if buy_broker == sell_broker: continue
            buy_ask = price_matrix.at[buy_broker, 'ask']
            if buy_ask is None or np.isnan(buy_ask): continue
            div = sell_bid - buy_ask
            if div >= threshold:
                qualifying.append((sell_broker, buy_broker, div))
    return qualifying


def can_open_trade(open_trades, sell_broker, buy_broker, max_trades=2):
    """
    Return True if:
      - There is NOT already an open trade between sell_broker and buy_broker
      - Neither broker has >= max_trades open trades (as either buy or sell leg)
    Assumes open_trades is a list of (sell_leg, buy_leg) tuples, both with .broker and .status attributes.
    """

    # Check if the pair already exists
    for sell_leg, buy_leg in open_trades:
        if (sell_leg.broker == sell_broker and buy_leg.broker == buy_broker
            and sell_leg.status == "open" and buy_leg.status == "open"):
            return False  # Trade already exists

    # Count open trades for each broker
    sell_broker_open = 0
    buy_broker_open = 0
    for sell_leg, buy_leg in open_trades:
        if sell_leg.broker == sell_broker and sell_leg.status == "open":
            sell_broker_open += 1
        if buy_leg.broker == sell_broker and buy_leg.status == "open":
            sell_broker_open += 1
        if sell_leg.broker == buy_broker and sell_leg.status == "open":
            buy_broker_open += 1
        if buy_leg.broker == buy_broker and buy_leg.status == "open":
            buy_broker_open += 1

    if sell_broker_open >= max_trades or buy_broker_open >= max_trades:
        return False

    return True


def calculate_lots(broker, balances_df, asset_conf, current_div, max_trades=2):
    """
    Lot sizing so max drawdown on this trade is never more than per-trade allocation.
    """
    allocated_capital = asset_conf["capital_allocation"] * balances_df[broker]
    capital_per_trade = allocated_capital / max_trades
    max_div = asset_conf["max_divergence"]
    move_to_stop = max_div - current_div
    if move_to_stop <= 0:
        return 0
    
    raw_lot = capital_per_trade / move_to_stop
    if raw_lot < asset_conf["min_lot"]:
        return 0.0
    
    steps = math.floor(raw_lot / asset_conf["min_lot"])
    return round(steps * asset_conf["min_lot"], 2)

                     
def init_trade(broker, counter_party, side, lot, price_matrix, slip, arb_id=None):
    if not arb_id:
        u = uuid.uuid4()
        h = hashlib.sha256(u.bytes).digest()
        arb_id = int.from_bytes(h[:4], 'big') & 0x7FFFFFFF  # 31-bit positive int

    if side == "sell":
        entry_price = price_matrix.loc[broker, "bid"]
    elif side == "buy":
        entry_price = price_matrix.loc[broker, "ask"]
    else:
        print("Unknown position side for trade in open_trade().")
        return None

    return Trade(
        arb_id=arb_id,
        broker=broker,
        counter_party=counter_party,
        side=side,
        allowed_slip=slip,
        lot_size=lot,
        entry_price=entry_price,
    )                     


def place_trade_pair(sell_trade, buy_trade, worker_cmd_queues, worker_resp_queues):
    """
    Send trade open requests to both brokers' workers and wait for confirmation.
    Returns (sell_leg, buy_leg) on success, None on failure.
    """
    sell_broker = sell_trade.broker
    buy_broker = buy_trade.broker

    # Send open trade commands
    worker_cmd_queues[sell_broker].put({"action": "open_trade", "trade": sell_trade})
    worker_cmd_queues[buy_broker].put({"action": "open_trade", "trade": buy_trade})

    # Wait for responses
    sell_trade_resp = worker_resp_queues[sell_broker].get()
    buy_trade_resp = worker_resp_queues[buy_broker].get()

    # Check responses
    if sell_trade_resp.status == "open" and buy_trade_resp.status == "open":
        logger.info(f"Trade pair opened successfully: {sell_broker}<->{buy_broker}")
        telebot.open_success(sell_trade_resp, buy_trade_resp)
    else:
        logger.warning(
            f"Trade pair failed! sell: {sell_trade_resp.status} ({sell_trade_resp.error}), "
            f"buy: {buy_trade_resp.status} ({buy_trade_resp.error})"
        )
        telebot.open_fail(sell_trade_resp, buy_trade_resp)
        # Flatten any "orphan" leg
        if sell_trade_resp.status == "open":
            logger.warning(f"Orphan sell leg opened on {sell_broker}. Attempting immediate close...")
            sell_trade_resp = close_leg(sell_trade_resp, worker_cmd_queues, worker_resp_queues)
            buy_trade_resp.status = "closed"
            telebot.open_orphan(sell_trade_resp)
        if buy_trade_resp.status == "open":
            logger.warning(f"Orphan buy leg opened on {buy_broker}. Attempting immediate close...")
            buy_trade_resp.status = close_leg(buy_trade_resp, worker_cmd_queues, worker_resp_queues)
            sell_trade_resp.status == "closed"
            telebot.open_orphan(buy_trade_resp)

    return (sell_trade_resp, buy_trade_resp)


def close_available_trades(open_trades, closed_trades, price_matrix, worker_cmd_queues, worker_resp_queues):
    if len(open_trades) == 0:
        return closed_trades, open_trades
    
    to_remove_indices = []
    updated_closed_trades = []

    for idx in range(len(open_trades)):
        sell_tr, buy_tr = open_trades[idx]

        if sell_tr.status == "pending_close":
            sell_tr = close_leg(sell_tr, worker_cmd_queues, worker_resp_queues)
        if buy_tr.status == "pending_close":
            buy_tr = close_leg(buy_tr, worker_cmd_queues, worker_resp_queues)

        # Check if trade should be closed now
        if sell_tr.status == "open" and buy_tr.status == "open" \
           and min_trade_time_passed(sell_tr.open_time) \
           and mean_reverted(sell_tr.broker, buy_tr.broker, price_matrix):
            sell_tr = close_leg(sell_tr, worker_cmd_queues, worker_resp_queues)
            buy_tr = close_leg(buy_tr, worker_cmd_queues, worker_resp_queues)

        if sell_tr.status == "closed" and buy_tr.status == "closed":
            updated_closed_trades.append((sell_tr, buy_tr))
            to_remove_indices.append(idx)
            telebot.close_trade(sell_tr, buy_tr)

    # Remove by index in reverse to avoid shifting
    for idx in sorted(to_remove_indices, reverse=True):
        del open_trades[idx]

    closed_trades.extend(updated_closed_trades)
    return closed_trades, open_trades


def mean_reverted(sell_broker, buy_broker, price_matrix):
    """
    Returns True if the current divergence between sell and buy brokers has mean-reverted (i.e., no longer positive arbitrage).
    For a 'sell' leg, you want to close if bid_sell - ask_buy <= 0.
    """
    sell_ask = price_matrix.loc[sell_broker, "ask"]
    buy_bid = price_matrix.loc[buy_broker, "bid"]
    divergence = buy_bid - sell_ask
    return divergence <= 0


def min_trade_time_passed(open_time):
    """
    Returns True if at least min_seconds have passed since open_time (both in UTC).
    open_time: ISO format string, e.g. '2024-06-28T22:15:05.624328+00:00'
    """
    if open_time is None:
        return False
    
    min_seconds = random.randrange(180, 240)
    
    dt_open = datetime.fromisoformat(open_time)
    now = datetime.now(UTC)
    elapsed = (now - dt_open).total_seconds()
    return elapsed >= min_seconds


def close_leg(trade, worker_cmd_queues, worker_resp_queues):
    """
    Request the worker to close the given trade and wait for the response.
    Returns the updated trade object (with new status).
    Assumes worker queues are labeled by broker name.
    """
    if trade.status == "closed":
        return trade
    
    broker = trade.broker
    worker_cmd_queues[broker].put({"action": "close_trade", "trade": trade})
    result_trade = worker_resp_queues[broker].get()
    return result_trade


def request_broker_positions(worker_cmd_queues, worker_resp_queues):
    """
    Ask each worker for their currently open broker-side positions.

    Returns:
        dict[str, list[Position]]: A mapping from broker name to list of Position objects.
    """
    # Send request to each broker
    for broker, cmd_q in worker_cmd_queues.items():
        cmd_q.put({"action": "get_open_positions"})

    # Collect responses
    broker_positions = {}
    for broker, resp_q in worker_resp_queues.items():
        try:
            resp = resp_q.get()
            broker_positions[broker] = resp.get("positions", [])
        except Exception as e:
            logger.error(f"[master] Failed to get positions from {broker}: {e}")
            broker_positions[broker] = []

    return broker_positions


def sync_arb_trades(closed_trades, open_trades, broker_positions):
    """
    Reconcile master’s open_trades with broker_positions.
    Returns (new_closed_trades, new_open_trades).
    """

    # Prepare quick lookups: broker -> set of magic IDs
    open_ids_by_broker = {
        broker: {pos["magic"] for pos in positions}
        for broker, positions in broker_positions.items()
    }

    # Begin with a shallow copy of closed_trades
    new_closed = closed_trades.copy()
    new_open   = []

    # Iterate internal open pairs
    for sell_tr, buy_tr in open_trades:
        sell_open = sell_tr.arb_id in open_ids_by_broker.get(sell_tr.broker, set())
        buy_open  = buy_tr.arb_id  in open_ids_by_broker.get(buy_tr.broker,  set())

        if not sell_open and  buy_open:
            # sell leg went away, buy still there → close sell, retry buy
            sell_tr.status = "closed"
            buy_tr .status = "pending_close"
            new_open.append((sell_tr, buy_tr))

        elif sell_open and not buy_open:
            # buy leg went away, sell still there → close buy, retry sell
            buy_tr .status = "closed"
            sell_tr.status = "pending_close"
            new_open.append((sell_tr, buy_tr))

        elif not sell_open and not buy_open:
            # both legs closed at broker → close both in master
            sell_tr.status = "closed"
            buy_tr .status = "closed"
            new_closed.append((sell_tr, buy_tr))

        else:
            # both still open
            new_open.append((sell_tr, buy_tr))

    return new_closed, new_open


def clean_rogue_trades(open_trades, open_lim_trades, broker_positions,
                        worker_cmd_queues, worker_resp_queues):
    """
    Finds any live broker positions whose magic IDs aren’t in open_trades
    or open_lim_trades, and closes them.
    """
    arb_ids = {tr.arb_id for s,b in open_trades for tr in (s,b)}
    lim_ids = {tr.arb_id for tr in open_lim_trades}
    tracked_ids = arb_ids | lim_ids

    for broker, positions in broker_positions.items():
        for pos in positions:
            magic = pos["magic"]
            if magic in tracked_ids:
                continue

            logger.warning(f"[sync] Rogue position on {broker}: magic={magic}, closing now..")
            stub = Trade(
                arb_id=magic,
                broker=broker,
                ticket=pos["ticket"],
                lot_size=pos["volume"],
                side=pos["side"],
                allowed_slip=10
            )
            close_leg(stub, worker_cmd_queues, worker_resp_queues)
