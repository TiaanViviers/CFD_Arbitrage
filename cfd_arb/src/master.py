import time
import pandas as pd
import numpy as np

from trade import Trade

TRADE_ID = 1

def master_proc(asset_config, worker_cmd_queues, worker_resp_queues, logger):
    open_trades = []
    while True:
        # data collection and formatting
        request_worker_ticks(worker_cmd_queues)
        ticks = get_worker_ticks(worker_resp_queues)
        price_matrix = build_price_matrix(ticks)

        # trade opening
        request_open_trade(asset_config, price_matrix, price_matrix, open_trades, worker_cmd_queues)
        #open_trades = get_opened_trades(worker_resp_queues)

        # trade closing
        #request_close_trade(open_trades, worker_cmd_queues)
        #open_trades = get_closed_trades(worker_resp_queues)


def request_worker_ticks(worker_cmd_queues):
    for q in worker_cmd_queues:
        q.put({"action": "get_tick"})


def get_worker_ticks(worker_resp_queues):
    ticks = {}
    for i, resp_q in enumerate(worker_resp_queues):
            resp = resp_q.get()
            ticks[resp["broker"]] = resp["tick"]
    return ticks


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


def request_open_trade(asset_config, price_matrix, open_trades, worker_cmd_queues):

    candidates = find_divergent_pairs(price_matrix, asset_config["entry_threshold"])
    if not candidates: return

    for sell_broker, buy_broker, div in candidates:
        if can_open_trade(open_trades, sell_broker, buy_broker):

    #if we can, create and populate a trade object
        #start by making a lot size calculation using divergence and available capital
        #if lot size < min_lot_size, we do not place or create a trade object
        #Once we have lot size, calculate sl using max_divergence and divergence

    #place it on worker_cmd_queue


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


def can_open_trade(open_trades, sell_broker, buy_broker):
    """
    Return True if there is NOT already an open trade between sell_broker and buy_broker.
    open_trades: list of (sell_leg, buy_leg) tuples.
    **IMPORTANT** Asumes open_trade tuples is stored as (sell_leg, buy_leg)
    """
    for sell_leg, buy_leg in open_trades:
        if (sell_leg.broker == sell_broker and buy_leg.broker == buy_broker
            and sell_leg.status == "open" and buy_leg.status == "open"):
            return False
    return True