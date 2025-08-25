"""
Worker process: executes broker actions for CFD Arbitrage system.

- Handles tick fetch, order open/close, and position queries.
- Communicates with master process via command/response queues.
"""

import logging
import sys
import time
from datetime import datetime, UTC
import math

from mt5_broker import MT5BrokerInterface
from trade import Trade

########################## Worker Process Entrypoint ###########################
def worker_proc(broker_conf, cmd_queue, resp_queue):
    logger = _setup_logger()
    broker = _init_broker(broker_conf, logger)

    while True:
        cmd = cmd_queue.get()
        action = cmd.get("action")
        if action == "get_tick":
            _get_tick(broker, resp_queue)
        elif action == "open_trade":
            _handle_open_trade(broker, cmd["trade"], resp_queue)
        elif action == "pnl_update":
            _handle_pnl_batch(broker, cmd["arb_ids"], resp_queue, logger)
        elif action == "close_trade":
            _handle_close_trade(broker, cmd["trade"], resp_queue, logger)
        elif action == "get_open_positions":
            _handle_get_open_positions(broker, resp_queue, logger)
        elif action == "shutdown":
            logger.info(f"Exiting worker for broker {broker.name}")
            broker.shutdown()
            break


################################ Broker Setup ##################################
def _init_broker(broker_config, logger) -> MT5BrokerInterface:
    """Create MT5BrokerInterface from config."""
    return MT5BrokerInterface(
        name=broker_config['broker'],
        path=broker_config['terminal_path'],
        symbol=broker_config['symbols'][0]['broker_symbol'],
        logger=logger
    )


################################ Command Handlers ################################
def _get_tick(broker, resp_queue) -> None:
    """
    Put latest tick + balance + max_lot on the response queue.
    One account-info call, one tick call — minimal latency.
    """
    snap = broker.get_capital_state()
    tick = broker.get_latest_tick()

    resp_queue.put({
        "type":    "tick",
        "broker":  broker.name,
        "tick":    tick,
        "balance": snap["balance"],
        "max_lot": snap["max_lot"],
    })


def _handle_open_trade(broker, trade, resp_queue) -> None:
    """
    Attempt to open a trade, update trade object with result, put on response queue.
    """
    try:
        trade.lot_size = _scale_exposure(broker, trade.lot_size)
        # Place order, with or without SL/TP
        if trade.sl is not None and trade.tp is not None:
            result = broker.place_order(
                side=trade.side, lots=trade.lot_size, price=trade.entry_price,
                deviation=_get_deviation(broker.digits, trade.allowed_slip), magic=trade.arb_id,
                sl=trade.sl, tp=trade.tp
            )
        else:
            result = broker.place_order(
                side=trade.side, lots=trade.lot_size, price=trade.entry_price,
                deviation=_get_deviation(broker.digits, trade.allowed_slip), magic=trade.arb_id
            )
        time.sleep(0.5)
        trade = _update_trade_after_open(broker, trade)

    except Exception as e:
        trade.ticket = None
        trade.status = "closed"
        trade.close_time = datetime.now(UTC).isoformat()
        trade.error = str(e)
        print(f"[{broker.name}] Failed to open trade: {e}")
    resp_queue.put({
        "type": "opened_trade",
        "trade": trade,
    })


def _handle_pnl_batch(broker, arb_ids, resp_queue, logger) -> None:
    pnl_dict = {}
    positions = broker.get_open_positions()
    if positions is not None:
        for pos in positions:
            magic = pos["magic"]
            if magic in arb_ids:
                pnl_dict[magic] = pos["profit"]

    resp_queue.put({
        "type": "pnl_update",
        "pnl": pnl_dict
    })


def _handle_close_trade(broker, trade, resp_queue, logger) -> None:
    """
    Attempt to close a trade, update trade object with result, put on response queue.
    """
    # If already closed, just return
    if trade.exit_price is not None or trade.close_time is not None:
        trade.status = "closed"
        trade.close_time = datetime.now(UTC).isoformat()
        resp_queue.put({
            "type": "closed_trade",
            "trade": trade,
        })
        return

    try:
        result = broker.close_position(
            ticket=trade.ticket,
            volume=trade.lot_size,
            deviation=_get_deviation(broker.digits, trade.allowed_slip),
            magic=trade.arb_id
        )
        trade = _update_trade_after_close(result, broker, trade, logger)

    except Exception as e:
        trade.status = "pending_close"
        trade.error = str(e)
    resp_queue.put({
        "type": "closed_trade",
        "trade": trade,
    })


def _handle_get_open_positions(broker, resp_queue, logger) -> None:
    """Put list of open positions on response queue."""
    try:
        positions = broker.get_open_positions()
        resp_queue.put({
            "type": "positions",
            "positions": positions
        })
    except Exception as e:
        logger.error(f"[{broker.name}] Failed to get open positions: {e}")
        resp_queue.put({"positions": []})


################################ Result Updaters ################################
def _update_trade_after_open(broker, trade) -> Trade:
    """
    Update trade fields after attempting to open.
    """
    positions = broker.get_positions()
    my_positions = [p for p in positions if p.magic == trade.arb_id]
    if my_positions:
        pos = my_positions[0]
        trade.ticket = pos.ticket
        trade.entry_price = pos.price_open
        trade.open_time = datetime.now(UTC).isoformat()
        trade.asset = broker.symbol
        trade.status = "open"
        trade.error = None
    else:
        trade.ticket = None
        trade.status = "closed"
        trade.close_time = datetime.now(UTC).isoformat()
        trade.error = "No position found after order!"
        broker.set_timeout()
    return trade


def _update_trade_after_close(result, broker, trade, logger) -> None:
    """
    Update trade fields after attempting to close.
    """
    if result is not None:
        trade.exit_price = getattr(result, "price", None)
        trade.close_time = datetime.now(UTC).isoformat()
        trade.status = "closed"
        trade.error = None
        deal_ticket = getattr(result, "deal", 0)
        broker_pnl = broker.get_trade_profit(deal_ticket=deal_ticket)
        if broker_pnl is not None:
            trade.pnl = broker_pnl
        else:
            mult = broker.contract_size or 1.0
            if trade.exit_price and trade.entry_price:
                if trade.side == "buy":
                    trade.pnl = (trade.exit_price - trade.entry_price) * trade.lot_size * mult
                else:
                    trade.pnl = (trade.entry_price - trade.exit_price) * trade.lot_size * mult
        logger.info(f"Successfully closed {trade.side} on {trade.broker} for ${trade.pnl}")
    else:
        trade.status = "pending_close"
    return trade


################################ Helpers ################################
def _get_deviation(digits, allowed_slip) -> int:
    """
    MT5 order_send deviation value for given allowed_slip and symbol decimals.
    """
    if digits is None:
        raise ValueError("Digits cannot be None when calculating deviation")
    point = 10 ** -digits
    return int(allowed_slip / point)


def _scale_exposure(broker: MT5BrokerInterface, lot_size: float) -> float:
        """
        Convert baseline $/point exposure into this broker's valid lot size.
        Floors to volume_step; returns 0.0 if below min_lot.
        """
        if lot_size <= 0 or not broker.vpp or broker.vpp <= 0:
            return 0.0
        if broker.vpp == 1:
            return lot_size
        
        step   = broker.volume_step or 0.01
        min_lot = broker.min_lot or step
        raw    = lot_size / broker.vpp
        lots   = math.floor(raw / step) * step

        return lots if lots >= min_lot else 0.0


def _setup_logger() -> logging.Logger:
    """
    Set up a logger to stdout, UTC timestamps, reload safe.
    """
    logger = logging.getLogger("arbitrage_bot")
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    fmt = '[%(asctime)s UTC] %(levelname)s %(name)s (%(threadName)s): %(message)s'
    logging.Formatter.converter = time.gmtime
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)
    return logger
