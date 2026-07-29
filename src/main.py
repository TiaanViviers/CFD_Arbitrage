"""
Entrypoint for the CFD Arbitrage trading system.

- Parses arguments (asset to trade)
- Sets up logging (stdout, UTC)
- Loads configs
- Spawns worker processes (one per broker)
- Launches master process, then gracefully shuts down workers
"""

from pathlib import Path

from dotenv import load_dotenv

# Load secrets before importing modules that read env (e.g. TeleBot).
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import sys
import time
import logging
import argparse
import multiprocessing as mp

from io_utils import load_broker_config, load_asset_config
from worker import worker_proc
from master import master_proc

############################### Main Entrypoint ################################
def main() -> None:
    """
    Parse args, setup logger, spawn workers, run master, shutdown.
    """
    args = _parse_args()
    _setup_logger()
    broker_cfg = load_broker_config(args.asset)
    asset_cfg = load_asset_config(args.asset)
    workers, cmd_qs, resp_qs = _start_workers(broker_cfg)
    try:
        master_proc(args.asset, asset_cfg, cmd_qs, resp_qs)
    finally:
        _shutdown_workers(workers)


############################### Startup Helpers ################################
def _parse_args():
    """Parse CLI args (asset selection)."""
    assets = ["BTCUSD", "GER40", "JP225", "US30", "US100"]
    parser = argparse.ArgumentParser(description="CFD Arbitrage Trading Bot")
    parser.add_argument("--asset", required=True, choices=assets,
                        help=f"Asset to trade, supported: {assets}")
    return parser.parse_args()


def _setup_logger() -> None:
    """Configure root logger: INFO to stdout, UTC timestamps, clean reload."""
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


def _start_workers(broker_cfg):
    """
    Spawn worker process per broker, with command/response queues.
    Returns: (worker_procs, cmd_queues, resp_queues)
    """
    worker_cmd_queues = {}
    worker_resp_queues = {}
    worker_procs = {}
    for broker_conf in broker_cfg:
        broker = broker_conf['broker']
        cmd_q = mp.Queue()
        resp_q = mp.Queue()
        p = mp.Process(target=worker_proc, args=(broker_conf, cmd_q, resp_q))
        p.start()
        worker_cmd_queues[broker] = cmd_q
        worker_resp_queues[broker] = resp_q
        worker_procs[broker] = p
    return worker_procs, worker_cmd_queues, worker_resp_queues


def _shutdown_workers(workers: dict) -> None:
    """Gracefully terminate/join all worker processes."""
    for broker, proc in workers.items():
        proc.join(timeout=5)
        if proc.is_alive():
            proc.terminate()
            proc.join()


if __name__ == "__main__":
    main()
