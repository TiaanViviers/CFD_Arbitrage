"""
master.py: Main arbitrage engine loop and trade orchestration.

Orchestrates tick data collection, trade entry/exit, state sync, and cleaning.
Groups functions by phase for clarity and maintainability.
"""
from datetime import datetime, UTC
from queue import Empty
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
import io_utils as io
from lim import open_lim, sync_lim_trades
from trading_schedules import is_trading_time

logger = logging.getLogger("arbitrage_bot")
telebot = TeleBot()


################################# Master Loop ##################################
def master_proc(asset: str, asset_config: dict, worker_cmd_queues: dict, 
                worker_resp_queues: dict) -> None:
    """
    Main trading loop: collect ticks, manage trades, sync, and clean state.

    - Checks if market is open
    - Collects tick and balance data from workers
    - Opens eligible arb and LIM trades
    - Closes trades that meet exit criteria
    - Syncs state with broker positions, cleans rogue trades
    """
    open_trades: list = []
    closed_trades: list = []
    open_lim_trades: list = []
    closed_lim_trades: list = []
    balances_df: pd.Series = {}
    telebot.set_asset(asset)

    try:
        while True:
            # ---------- Market open check ----------
            if not is_trading_time(asset):
                _daily_update(closed_trades, closed_lim_trades, balances_df, telebot)
                time.sleep(10)
                continue

            # ---------- Data collection ----------
            _request_worker_ticks(worker_cmd_queues)
            price_matrix, balances_df, maxlot_series = _get_worker_ticks(worker_resp_queues)

            # ---------- Trade opening ----------
            open_trades = _open_available_trades(
                asset_config, price_matrix, balances_df, maxlot_series, 
                open_trades, worker_cmd_queues, worker_resp_queues
            )
            open_lim_trades = open_lim(
                open_lim_trades, closed_lim_trades, closed_trades, asset_config,
                balances_df, price_matrix, worker_cmd_queues, worker_resp_queues, telebot
            )

            # ---------- Trade updating ----------
            if len(open_trades) > 0:
                requested = _request_pnl_update(worker_cmd_queues, open_trades)
                pnl_dict = _get_pnl_update(worker_resp_queues, requested)
                open_trades = update_trades(open_trades, pnl_dict)
            
            # ---------- Trade closing ----------
            closed_trades, open_trades = _close_available_trades(
                open_trades, closed_trades, asset_config,
                worker_cmd_queues, worker_resp_queues
            )

            # ---------- Sync and cleaning ----------
            broker_positions = _request_broker_positions(
                worker_cmd_queues, worker_resp_queues
            )
            closed_trades, open_trades = _sync_arb_trades(
                closed_trades, open_trades, broker_positions, worker_cmd_queues,
                worker_resp_queues
            )
            closed_lim_trades, open_lim_trades = sync_lim_trades(
                closed_lim_trades, open_lim_trades, broker_positions,
                price_matrix, telebot
            )
            _clean_rogue_trades(
                open_trades, open_lim_trades, broker_positions,
                worker_cmd_queues, worker_resp_queues
            )

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down.")
        io.write_closed_trades(asset, closed_trades, closed_lim_trades)
        _shutdown_workers(worker_cmd_queues)
        

############################# Worker Communication #############################
def _shutdown_workers(worker_cmd_queues: dict) -> None:
    """
    Send shutdown signal to all worker processes.
    """
    for cmd_q in worker_cmd_queues.values():
        cmd_q.put({"action": "shutdown"})
    logger.info("Shutdown commands sent. Exiting master_proc.")


def _request_worker_ticks(worker_cmd_queues: dict) -> None:
    """
    Request latest tick/balance from all workers.
    """
    for q in worker_cmd_queues.values():
        q.put({"action": "get_tick"})


def _get_worker_ticks(worker_resp_queues: dict) -> tuple[pd.DataFrame, pd.Series]:
    """
    Collect tick and balance responses from all workers.
    Returns (price_matrix, balances_df).
    """
    ticks = {}
    balances = {}
    max_lots = {}

    for broker, resp_q in worker_resp_queues.items():
        resp              = resp_q.get()
        ticks[broker]     = resp.get("tick")
        balances[broker]  = resp.get("balance")
        max_lots[broker]  = resp.get("max_lot", 0.0)

    price_matrix = _build_price_matrix(ticks)
    balances_df = pd.Series(balances, name="balance").sort_index()
    maxlot_series = pd.Series(max_lots,  name="max_lot").sort_index()
    return price_matrix, balances_df, maxlot_series


def _request_pnl_update(worker_cmd_queues: dict, open_trades: list) -> set:
    """
    Request PnL updates in batches for all open trades from workers.
    Returns the set of brokers we actually messaged.
    """
    broker_to_arb_ids = {}

    for trade_pair in open_trades:
        for trade in trade_pair:
            broker_to_arb_ids.setdefault(trade.broker, set()).add(trade.arb_id)

    for broker, arb_ids in broker_to_arb_ids.items():
        worker_cmd_queues[broker].put({
            "action": "pnl_update",
            "arb_ids": list(arb_ids)
        })

    return set(broker_to_arb_ids)


def _get_pnl_update(worker_resp_queues: dict,
                    requested_brokers: set) -> dict:
    """
    Collect PnL updates only from brokers we asked.
    """
    
    pnl_dict = {}
    for broker in requested_brokers:
        resp_q = worker_resp_queues[broker]
        try:
            resp = resp_q.get(timeout=1)
            pnl_dict.update(resp.get("pnl", {}))
        except Empty:
            logger.warning(f"[master] no PnL response from {broker}")
        except Exception as e:
            logger.error(f"[master] error reading PnL from {broker}: {e}")
    return pnl_dict


def _build_price_matrix(ticks: dict) -> pd.DataFrame:
    """
    Convert dict of ticks to DataFrame with columns ['bid', 'ask'].
    """
    clean_ticks = {b: t for b, t in ticks.items() if t is not None}
    if not clean_ticks:
        return pd.DataFrame(columns=["bid", "ask"])
    df = pd.DataFrame.from_dict(clean_ticks, orient="index")
    return df[["bid", "ask"]].sort_index()


################################ Trade Opening #################################
def _open_available_trades(asset_config: dict, price_matrix: pd.DataFrame, 
                           balances_df: pd.Series, maxlot_series: pd.Series,
                           open_trades: list,worker_cmd_queues: dict,
                           worker_resp_queues: dict) -> list:
    """
    Attempt to open eligible arbitrage trades based on divergence and limits.
    """
    candidates = _find_divergent_pairs(price_matrix, asset_config["entry_threshold"])
    if not candidates:
        return open_trades

    for sell_broker, buy_broker, div in candidates:
        if _can_open_trade(open_trades, sell_broker, buy_broker):
            sell_lot = _calculate_lots(sell_broker, balances_df, maxlot_series, asset_config, div)
            buy_lot = _calculate_lots(buy_broker, balances_df, maxlot_series, asset_config, div)
            lot_size = min(sell_lot, buy_lot)
            if lot_size < asset_config["min_lot"]:
                continue
            sell_trade = _init_trade(
                sell_broker, buy_broker, "sell", lot_size,
                price_matrix, asset_config["allowed_slip"]
            )
            arb_id = sell_trade.arb_id
            buy_trade = _init_trade(
                buy_broker, sell_broker, "buy", lot_size,
                price_matrix, asset_config["allowed_slip"], arb_id=arb_id
            )
            trade_pair = _place_trade_pair(
                sell_trade, buy_trade, worker_cmd_queues, worker_resp_queues
            )
            open_trades.append(trade_pair)

    return open_trades


def _find_divergent_pairs(price_matrix: pd.DataFrame, threshold: float
                    ) -> list[tuple[str, str, float]]:
    """
    Find all broker pairs with divergence >= threshold.
    Returns list of (sell_broker, buy_broker, divergence).
    """
    qualifying = []
    brokers = price_matrix.index
    for sell_broker in brokers:
        sell_bid = price_matrix.at[sell_broker, 'bid']
        if sell_bid is None or np.isnan(sell_bid):
            continue
        for buy_broker in brokers:
            if buy_broker == sell_broker:
                continue
            buy_ask = price_matrix.at[buy_broker, 'ask']
            if buy_ask is None or np.isnan(buy_ask):
                continue
            div = sell_bid - buy_ask
            if div >= threshold:
                qualifying.append((sell_broker, buy_broker, div))
    return qualifying


def _can_open_trade(open_trades: list, sell_broker: str, 
                    buy_broker: str, max_trades: int = 2) -> bool:
    """
    Return True if this broker pair is eligible for a new trade.
    No existing open pair, neither broker at max open trades.
    """
    # Check for existing open pair
    for sell_leg, buy_leg in open_trades:
        if (sell_leg.broker == sell_broker and buy_leg.broker == buy_broker
            and sell_leg.status == "open" and buy_leg.status == "open"):
            return False

    # Count open trades for each broker
    sell_broker_open = sum(
        (sell_leg.broker == sell_broker and sell_leg.status == "open") or
        (buy_leg.broker == sell_broker and buy_leg.status == "open")
        for sell_leg, buy_leg in open_trades
    )
    buy_broker_open = sum(
        (sell_leg.broker == buy_broker and sell_leg.status == "open") or
        (buy_leg.broker == buy_broker and buy_leg.status == "open")
        for sell_leg, buy_leg in open_trades
    )
    return sell_broker_open < max_trades and buy_broker_open < max_trades


def _calculate_lots(broker: str, balances_df: pd.Series, maxlot_series: pd.Series, 
                    asset_conf: dict, current_div: float, max_trades: int = 2) -> float:
    """
    Lot sizing so max drawdown on this trade is never more than per-trade allocation.
    """
    allocated_capital = asset_conf["capital_allocation"] * balances_df[broker]
    capital_per_trade = allocated_capital / max_trades
    max_div = asset_conf["max_divergence"]
    move_to_stop = max_div - current_div
    if move_to_stop <= 0:
        return 0.0

    raw_lot = capital_per_trade / move_to_stop
    steps = math.floor(raw_lot / asset_conf["min_lot"])

    lot_cap = round(steps * asset_conf["min_lot"], 2)
    margin_cap =  maxlot_series.get(broker, 0.0)
    final_lot = min(lot_cap, margin_cap)

    if final_lot < asset_conf["min_lot"]:
        return 0.0
    return final_lot


def _init_trade(broker: str, counter_party: str, side: str, lot: float,
                price_matrix: pd.DataFrame, slip: float, arb_id: int = None
            ) -> Trade:
    """
    Create Trade object for this leg of the arb.
    """
    if not arb_id:
        u = uuid.uuid4()
        h = hashlib.sha256(u.bytes).digest()
        arb_id = int.from_bytes(h[:4], 'big') & 0x7FFFFFFF

    entry_price = (
        price_matrix.loc[broker, "bid"] if side == "sell"
        else price_matrix.loc[broker, "ask"] if side == "buy"
        else None
    )
    if entry_price is None:
        raise ValueError("Unknown side for trade in _init_trade().")

    return Trade(
        arb_id=arb_id,
        broker=broker,
        counter_party=counter_party,
        side=side,
        allowed_slip=slip,
        lot_size=lot,
        entry_price=entry_price,
    )


def _place_trade_pair(sell_trade: Trade, buy_trade: Trade,
                      worker_cmd_queues: dict, worker_resp_queues: dict
                    ) -> tuple[Trade, Trade]:
    """
    Send open requests to both worker processes. Return both updated trades.
    """
    sell_broker = sell_trade.broker
    buy_broker = buy_trade.broker

    worker_cmd_queues[sell_broker].put({"action": "open_trade", "trade": sell_trade})
    worker_cmd_queues[buy_broker].put({"action": "open_trade", "trade": buy_trade})

    sell_trade_resp = worker_resp_queues[sell_broker].get()
    buy_trade_resp = worker_resp_queues[buy_broker].get()

    if sell_trade_resp.status == "open" and buy_trade_resp.status == "open":
        logger.info(f"Trade pair opened: {sell_broker}<->{buy_broker}")
        telebot.open_success(sell_trade_resp, buy_trade_resp)
    else:
        logger.warning(
            f"Trade pair failed! sell: {sell_trade_resp.status} ({sell_trade_resp.error}), "
            f"buy: {buy_trade_resp.status} ({buy_trade_resp.error})"
        )
        telebot.open_fail(sell_trade_resp, buy_trade_resp)
        # Close any orphan legs
        if sell_trade_resp.status == "open":
            logger.warning(f"Orphan sell leg on {sell_broker}, closing...")
            sell_trade_resp = _close_leg(sell_trade_resp, worker_cmd_queues, worker_resp_queues)
            buy_trade_resp.status = "closed"
        if buy_trade_resp.status == "open":
            logger.warning(f"Orphan buy leg on {buy_broker}, closing...")
            buy_trade_resp = _close_leg(buy_trade_resp, worker_cmd_queues, worker_resp_queues)
            sell_trade_resp.status = "closed"

    return (sell_trade_resp, buy_trade_resp)


################################ Trade Updating ################################
def update_trades(open_trades: list, pnl_dict: dict) -> tuple[list, list]:
    """
    Update open and protected trades with PnL from pnl_dict.
    """
    for idx, (sell_trade, buy_trade) in enumerate(open_trades):
        if sell_trade.arb_id in pnl_dict:
            sell_trade.pnl = pnl_dict[sell_trade.arb_id]
        if buy_trade.arb_id in pnl_dict:
            buy_trade.pnl = pnl_dict[buy_trade.arb_id]
        open_trades[idx] = (sell_trade, buy_trade)
    
    return open_trades


################################ Trade Closing #################################
def _close_available_trades(open_trades: list, closed_trades: list, asset_conf: dict,
                            worker_cmd_queues: dict, worker_resp_queues: dict
                        ) -> tuple[list, list]:
    """
    Check all open trades for exit criteria, close as needed, and update lists.
    """
    if not open_trades:
        return closed_trades, open_trades

    to_remove = []
    updated_closed = []

    for idx, (sell_tr, buy_tr) in enumerate(open_trades):
        # Attempt closing pending legs first
        if sell_tr.status == "pending_close":
            sell_tr = _close_leg(sell_tr, worker_cmd_queues, worker_resp_queues)
        if buy_tr.status == "pending_close":
            buy_tr = _close_leg(buy_tr, worker_cmd_queues, worker_resp_queues)

        # Attempt to lock in more profit on protected trades
        if sell_tr.status == "protected" and buy_tr.status == "protected":
            sell_tr = _protect_leg(sell_tr, asset_conf, worker_cmd_queues, worker_resp_queues)
            buy_tr = _protect_leg(buy_tr, asset_conf, worker_cmd_queues, worker_resp_queues)

        # Attempt to lock in profit of open trades
        if (sell_tr.status == "open" and buy_tr.status == "open"
            and _min_trade_time_passed(sell_tr.open_time)
            and _mean_reverted(sell_tr, buy_tr, asset_conf)
            ):
            sell_tr = _protect_leg(sell_tr, asset_conf, worker_cmd_queues, worker_resp_queues)
            buy_tr = _protect_leg(buy_tr, asset_conf, worker_cmd_queues, worker_resp_queues)

        # One leg is not protected, try protection with smaller margin
        if sell_tr.status == "open" and buy_tr.status == "protected":
           sell_tr = _protect_leg(sell_tr, asset_conf, worker_cmd_queues,
                                   worker_resp_queues, factor=0.25)
        if sell_tr.status == "protected" and buy_tr.status == "open":
            buy_tr = _protect_leg(buy_tr, asset_conf, worker_cmd_queues,
                                   worker_resp_queues, factor=0.25)
        
        # Move trades that are closed to closed_trades
        if sell_tr.status == "closed" and buy_tr.status == "closed":
            sell_tr, buy_tr = _finalize_pnl(sell_tr, buy_tr, worker_cmd_queues,
                                            worker_resp_queues)
            updated_closed.append((sell_tr, buy_tr))
            to_remove.append(idx)
            telebot.close_trade(sell_tr, buy_tr)

    # Remove closed from open_trades, in reverse to avoid index shifting
    for idx in sorted(to_remove, reverse=True):
        del open_trades[idx]

    closed_trades.extend(updated_closed)
    return closed_trades, open_trades


def _mean_reverted(sell_trade: Trade, buy_trade: Trade, asset_conf: dict) -> bool:
    """
    Return True if the trade has mean-reverted enough to justify taking profit.
    Includes a buffer margin to account for slippage.
    """
    current_pnl = sell_trade.pnl + buy_trade.pnl
    threshold = asset_conf["entry_threshold"]
    buffer_margin = asset_conf["buffer"] * sell_trade.lot_size + asset_conf["allowed_slip"]

    return current_pnl > abs(threshold - buffer_margin)


def _min_trade_time_passed(open_time: str) -> bool:
    """
    True if at least min_seconds (randomized 180–240s) have passed since open_time (ISO string).
    """
    if open_time is None:
        return False
    min_seconds = random.randrange(180, 240)
    dt_open = datetime.fromisoformat(open_time)
    now = datetime.now(UTC)
    elapsed = (now - dt_open).total_seconds()
    return elapsed >= min_seconds


def _close_leg(trade: Trade, worker_cmd_queues: dict, 
               worker_resp_queues: dict) -> Trade:
    """
    Request worker to close this trade, wait for response, return updated trade.
    """
    if trade.status == "closed":
        return trade
    broker = trade.broker
    worker_cmd_queues[broker].put({"action": "close_trade", "trade": trade})
    return worker_resp_queues[broker].get()


def _protect_leg(trade: Trade, asset_conf: dict, worker_cmd_queues: dict, 
                 worker_resp_queues: dict, factor: int = 0.5) -> Trade:
    """
    Attempt to add a stop loss to a trade that is already in profit.
    """
    if trade.side == "sell":
        trade.new_sl = trade.entry_price - (trade.pnl / trade.lot_size) + \
                        (asset_conf["buffer"] * factor * trade.lot_size)
    if trade.side == "buy":
        trade.new_sl = trade.entry_price + (trade.pnl / trade.lot_size) - \
                        (asset_conf["buffer"] * factor * trade.lot_size)
        
    # No need to update if new SL is worse/only small improvement
    if trade.status == "protected" and trade.side == "sell":
        if trade.new_sl > trade.sl - (trade.lot_size * asset_conf["buffer"]) - \
        asset_conf["allowed_slip"]:
            return trade  
    if trade.status == "protected" and trade.side == "buy":
        if trade.new_sl < trade.sl + (trade.lot_size * asset_conf["buffer"]) + \
        asset_conf["allowed_slip"]:
            return trade
        
    worker_cmd_queues[trade.broker].put({"action": "sl_update", "trade": trade})
    return worker_resp_queues[trade.broker].get()


def _finalize_pnl(sell_trade: Trade, buy_trade: Trade, worker_cmd_queues: dict, 
                 worker_resp_queues: dict) -> tuple[Trade, Trade]:
    """
    Finalize PnL for both legs of the trade, set close times.
    """
    worker_cmd_queues[sell_trade.broker].put({"action": "final_pnl", "trade": sell_trade})
    worker_cmd_queues[buy_trade.broker].put({"action": "final", "trade": buy_trade})
    sell_trade_resp = worker_resp_queues[sell_trade.broker].get()
    buy_trade_resp = worker_resp_queues[buy_trade.broker].get()
    return sell_trade_resp, buy_trade_resp
    

############################ Broker Synchronization ############################
def _request_broker_positions(
        worker_cmd_queues: dict, worker_resp_queues: dict) -> dict:
    """
    Ask each worker for its open broker-side positions.
    Returns: {broker_name: list[Position]}
    """
    for cmd_q in worker_cmd_queues.values():
        cmd_q.put({"action": "get_open_positions"})
    broker_positions = {}
    for broker, resp_q in worker_resp_queues.items():
        try:
            resp = resp_q.get()
            broker_positions[broker] = resp.get("positions", [])
        except Exception as e:
            logger.error(f"[master] Failed to get positions from {broker}: {e}")
            broker_positions[broker] = []
    return broker_positions


def _sync_arb_trades(closed_trades: list, open_trades: list,
                     broker_positions: dict, worker_cmd_queues: dict, 
                     worker_resp_queues: dict) -> tuple[list, list]:
    """
    Reconcile open_trades with broker_positions. Update statuses accordingly.
    Returns (new_closed_trades, new_open_trades).
    """
    open_ids = {
        broker: {pos["magic"] for pos in positions}
        for broker, positions in broker_positions.items()
    }
    new_closed = closed_trades.copy()
    new_open = []

    for sell_tr, buy_tr in open_trades:
        sell_open = sell_tr.arb_id in open_ids.get(sell_tr.broker, set())
        buy_open = buy_tr.arb_id in open_ids.get(buy_tr.broker, set())
        if not sell_open and buy_open:
            sell_tr.status = "closed"
            sell_tr.close_time = datetime.now(UTC).isoformat()
            if buy_tr.status == "open":
                buy_tr.status = "pending_close"
            new_open.append((sell_tr, buy_tr))

        elif sell_open and not buy_open:
            buy_tr.status = "closed"
            buy_tr.close_time = datetime.now(UTC).isoformat()
            if sell_tr.status == "open":
                sell_tr.status = "pending_close"
            new_open.append((sell_tr, buy_tr))

        elif not sell_open and not buy_open:
            sell_tr.status = "closed"
            buy_tr.status = "closed"
            sell_tr, buy_tr = _finalize_pnl(sell_tr, buy_tr, worker_cmd_queues,
                                             worker_resp_queues)
            sell_tr.close_time = datetime.now(UTC).isoformat()
            buy_tr.close_time = datetime.now(UTC).isoformat()
            new_closed.append((sell_tr, buy_tr))
            telebot.close_trade(sell_tr, buy_tr)
            
        else:
            new_open.append((sell_tr, buy_tr))
    return new_closed, new_open


def _clean_rogue_trades(open_trades: list, open_lim_trades: list, 
                        broker_positions: dict,worker_cmd_queues: dict, 
                        worker_resp_queues: dict) -> None:
    """
    Find live broker positions whose magic IDs aren't in tracked trades, and close them.
    """
    tracked_ids = {tr.arb_id for tr in open_lim_trades}
    for sell, buy in open_trades:
        tracked_ids.add(sell.arb_id)
        tracked_ids.add(buy.arb_id)
    for broker, positions in broker_positions.items():
        for pos in positions:
            magic = pos["magic"]
            if magic in tracked_ids:
                continue
            logger.warning(f"[sync] Rogue position on {broker}: magic={magic}, closing now..")
            stub = Trade(
                arb_id=magic, counter_party="Rogue", entry_price=0,
                broker=broker, ticket=pos["ticket"], lot_size=pos["volume"],
                side=pos["side"], allowed_slip=10
            )
            _close_leg(stub, worker_cmd_queues, worker_resp_queues)


############################ Daily Update ############################
def _daily_update(closed_arb_trades: list, closed_lim_trades: list,
                   balances: dict, telebot: TeleBot) -> None:
    """
    If time is 21:03 UTC, send a daily Telegram report.
    """
    now = datetime.now(UTC)
    if now.hour == 21 and now.minute == 3 and balances is not None:
        telebot.daily_report(closed_arb_trades, closed_lim_trades, balances)
        time.sleep(60)
