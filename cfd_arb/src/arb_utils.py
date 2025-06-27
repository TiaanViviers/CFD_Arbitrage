import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed

def build_price_matrix(brokers, logger, timeout=0.5):
    broker_names = list(brokers.keys())
    bids = np.full(len(broker_names), np.nan)
    asks = np.full(len(broker_names), np.nan)

    def get_tick(broker):
        try:
            return broker.get_latest_tick()
        except Exception as e:
            logger.warning(f"Error getting tick from {broker.name}: {e}")
            return None

    # Parallel fetch with threads
    with ThreadPoolExecutor(max_workers=len(broker_names)) as executor:
        futures = {
            executor.submit(get_tick, brokers[name]): idx
            for idx, name in enumerate(broker_names)
        }
        for future in as_completed(futures, timeout=timeout):
            idx = futures[future]
            tick = future.result()
            if tick is not None:
                bids[idx] = tick['bid']
                asks[idx] = tick['ask']

    return bids, asks
