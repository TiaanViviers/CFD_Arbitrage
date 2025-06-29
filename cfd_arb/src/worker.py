from mt5_broker import MT5BrokerInterface

def worker_proc(broker_conf, cmd_queue, resp_queue, logger):
    broker = init_broker(broker_conf, logger)

    while True:
        cmd = cmd_queue.get()
        if cmd["action"] == "get_tick":
            tick = broker.get_latest_tick()
            resp_queue.put({"type": "tick", "broker": broker_conf['broker'], "tick": tick})
        elif cmd["action"] == "place_order":
            # ... implement trade logic here ...
            pass
        elif cmd["action"] == "shutdown":
            break


def init_broker(broker_config, logger):
    return MT5BrokerInterface(
        name=broker_config['broker'],
        path=broker_config['terminal_path'],
        symbol=broker_config['symbols'][0]['broker_symbol'],
        logger=logger
    )
