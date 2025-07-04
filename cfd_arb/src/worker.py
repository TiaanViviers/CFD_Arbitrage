import logging
import time
from datetime import datetime, UTC

from mt5_broker import MT5BrokerInterface

logger = logging.getLogger("arbitrage_bot")


def worker_proc(broker_conf, cmd_queue, resp_queue):
    broker = init_broker(broker_conf, logger)

    while True:
        cmd = cmd_queue.get()
        if cmd["action"] == "get_tick":
            get_tick(broker, resp_queue)
            
        elif cmd["action"] == "open_trade":
            handle_open_trade(broker, cmd["trade"], resp_queue)
        
        elif cmd["action"] == "close_trade":
            handle_close_trade(broker, cmd["trade"], resp_queue)

        elif cmd["action"] == "shutdown":
            break


def get_tick(broker, resp_queue):
    resp_queue.put({"type": "tick",
                    "broker": broker.name,
                    "tick": broker.get_latest_tick(),
                    "balance": broker.get_balance()
                })


def init_broker(broker_config, logger):
    return MT5BrokerInterface(
        name=broker_config['broker'],
        path=broker_config['terminal_path'],
        symbol=broker_config['symbols'][0]['broker_symbol'],
        logger=logger
    )


def handle_open_trade(broker, trade, resp_queue):
    try:
        result = broker.place_order(
            side=trade.side,
            lots=trade.lot_size,
            price=trade.entry_price,
            sl=trade.sl,
            deviation=get_deviation(broker.digits, trade.allowed_slip),
            magic=trade.arb_id
        )
        time.sleep(0.5)

        positions = broker.get_positions()
        my_positions = [p for p in positions if p.magic == trade.arb_id]
        if my_positions:
            pos = my_positions[0]
            trade.ticket = pos.ticket
            trade.entry_price = pos.price_open
            trade.open_time = datetime.now(UTC).isoformat(),
            trade.asset = broker.symbol
            trade.status = "open"
            trade.error = None
            logger.info(f"Successfully opened {trade.side} on {trade.broker}")
        else:
            trade.ticket = None
            trade.status = "failed"
            trade.error = "No position found after order!"
    except Exception as e:
        trade.ticket = None
        trade.status = "failed"
        trade.error = str(e)
    resp_queue.put(trade)


def get_deviation(digits, allowed_slip):
    """
    Calculate the MT5 order_send deviation value for a given allowed_slip (in USD or quote currency)
    and number of decimal digits for the symbol.
    """
    point = 10 ** -digits
    return int(allowed_slip / point)


def handle_close_trade(broker, trade, resp_queue, max_attempts=10):
    attempts = 0
    while attempts < max_attempts:
        try:
            result = broker.close_position(
                ticket=trade.ticket,
                volume=trade.lot_size,
                deviation=get_deviation(broker.digits, trade.allowed_slip),
                magic=trade.arb_id
            )

            if result is not None:
                logger.info(f"Successfully closed {trade.side} on {trade.broker}")
                trade.exit_price = getattr(result, "price", None)
                trade.close_time = datetime.now(UTC).isoformat()
                trade.status = "closed"
                trade.error = None
                if trade.exit_price is not None and trade.entry_price is not None:
                    if trade.side == "buy":
                        trade.pnl = (trade.exit_price - trade.entry_price) * trade.lot_size
                    else:
                        trade.pnl = (trade.entry_price - trade.exit_price) * trade.lot_size
                logger.info(f"Successfully closed {trade.side} on {trade.broker} for ${trade.pnl}")

            else:
                trade.status = "close_failed"
                trade.error = f"Broker reported failure: {getattr(result, 'comment', 'no comment')}"

        except Exception as e:
            trade.status = "close_failed"
            trade.error = str(e)
        time.sleep(1)
        attempts += 1
    resp_queue.put(trade)

