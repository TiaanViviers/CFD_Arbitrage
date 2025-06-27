import logging
import sys
import time
import argparse

from io_utils import load_broker_config
from mt5_broker import MT5BrokerInterface
from arb_utils import build_price_matrix


def main():
    args = parse_args()
    logger = setup_logger()
    config = load_broker_config(args.asset)
    brokers = init_brokers(config, logger)

    run_arbitrage(brokers, logger)


def run_arbitrage(brokers, logger):
    while True:
        bids, asks = build_price_matrix(brokers, logger)

        print("\n\nBroker    |     Bid      |     Ask")
        print("-----------------------------------------")
        for i, name in enumerate(list(brokers.keys())):
            bid = bids[i]
            ask = asks[i]
            print(f"{name:<10} | {bid:10.5f} | {ask:10.5f}")


        #open_available_trades(div_matrix, brokers, logger)
        #close_available_trades(div_matrix, brokers,logger)
    


def init_brokers(config, logger):
    brokers = {}
    for broker_entry in config:
        brokers[broker_entry['broker']] = MT5BrokerInterface(
            name=broker_entry['broker'],
            path=broker_entry['terminal_path'],
            symbol=broker_entry['symbols'][0]['broker_symbol'],
            logger=logger
        )
    return brokers


def parse_args():
    assets = ["BTCUSD", "GER40", "JP225", "US30", "US100"]
    parser = argparse.ArgumentParser(description="CFD Arbitrage Trading Bot")
    parser.add_argument("--asset",required=True, choices=assets,
                        help=f"Asset to trade, supported: {assets}")

    return parser.parse_args()


def setup_logger() -> logging.Logger:
    """
    Set up a logger that only writes to stdout, suitable for long-running trading bots on a VPS.
    """
    logger = logging.getLogger("arbitrage_bot")
    logger.setLevel(logging.INFO)

    # Remove any existing handlers (reload safety)
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter('[%(asctime)s UTC] %(levelname)s %(name)s (%(threadName)s): %(message)s')
    logging.Formatter.converter = time.gmtime

    # Stream handler for stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

    return logger


if __name__ == "__main__":
    main()
