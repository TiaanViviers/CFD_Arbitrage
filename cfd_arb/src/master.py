import time
import pandas as pd
import numpy as np
import uuid

from trade import Trade

def master_proc(asset_config, worker_cmd_queues, worker_resp_queues, logger):
    open_trades = []
    while True:
        # data collection and formatting
        request_worker_ticks(worker_cmd_queues)
        price_matrix, balances_df = get_worker_ticks(worker_resp_queues)

        # trade opening
        request_open_trade(asset_config, price_matrix, balances_df, open_trades, worker_cmd_queues)
        #open_trades = get_opened_trades(worker_resp_queues)

        # trade closing
        #request_close_trade(open_trades, worker_cmd_queues)
        #open_trades = get_closed_trades(worker_resp_queues)


def request_worker_ticks(worker_cmd_queues):
    for q in worker_cmd_queues:
        q.put({"action": "get_tick"})


def get_worker_ticks(worker_resp_queues):
    ticks = {}
    balances = {}
    for resp_q in worker_resp_queues:
        resp = resp_q.get()
        broker = resp["broker"]
        ticks[broker] = resp["tick"]
        balances[broker] = resp["balance"]
    price_matrix = build_price_matrix(ticks)
    balances_df = pd.Series(balances, name="balance").sort_index()
    return price_matrix, balances_df


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


def request_open_trade(asset_config, price_matrix, balances_df, open_trades, worker_cmd_queues):

    candidates = find_divergent_pairs(price_matrix, asset_config["entry_threshold"])
    if not candidates: return

    for sell_broker, buy_broker, div in candidates:
        if can_open_trade(open_trades, sell_broker, buy_broker):
            sell_lot = calculate_lots(sell_broker, balances_df, asset_config, div)
            buy_lot = calculate_lots(buy_broker, balances_df, asset_config, div)
            lot_size = min(sell_lot, buy_lot)
            if lot_size < asset_config["min_lot"]:
                continue

            sell_sl = calculate_sl("sell", div, asset_config["max_divergence"], sell_broker, price_matrix)
            buy_sl = calculate_sl("buy", div, asset_config["max_divergence"], sell_broker, price_matrix)

            
            sell_trade = open_trade(sell_broker, buy_broker, "sell", lot_size, price_matrix, sell_sl)
            buy_trade = open_trade(buy_broker, sell_broker, "buy", lot_size, price_matrix, buy_sl)

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
    
    lot_size = round(capital_per_trade / move_to_stop, 2)
    return lot_size


def calculate_sl(side, current_div, max_div, broker, price_matrix):
    max_drawdown = max_div - current_div

    if side == "sell":
        entry_price = price_matrix.loc[broker, "bid"]
        sl = entry_price + max_drawdown
    else:
        entry_price = price_matrix.loc[broker, "ask"]
        sl = entry_price - max_drawdown
                                  
    return sl

                     
def open_trade(broker, counter_party, side, lot, price_matrix, sl, logger, arb_id=None):
    if not arb_id:
        arb_id = str(uuid.uuid4())

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
        lot_size=lot,
        entry_price=entry_price,
        sl=sl,
    )                     



