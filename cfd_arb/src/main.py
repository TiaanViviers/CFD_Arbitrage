import logging
import sys
import time
import argparse
import multiprocessing as mp

from io_utils import load_broker_config, load_asset_config
from worker import worker_proc
from master import master_proc


def main():
    args = parse_args()
    setup_logger()
    broker_config = load_broker_config(args.asset)
    asset_config = load_asset_config(args.asset)

    worker_cmd_queues = {}
    worker_resp_queues = {}
    for broker_conf in broker_config:
        broker_name = broker_conf['broker']
        cmd_q = mp.Queue()
        resp_q = mp.Queue()
        p = mp.Process(target=worker_proc, args=(broker_conf, cmd_q, resp_q))
        p.start()
        worker_cmd_queues[broker_name] = cmd_q
        worker_resp_queues[broker_name] = resp_q

    master_proc(args.asset, asset_config, worker_cmd_queues, worker_resp_queues)


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
