import time

def master_proc(worker_cmd_queues, worker_resp_queues, logger):
    while True:
        request_worker_ticks(worker_cmd_queues)
        ticks = get_worker_ticks(worker_resp_queues)

        print(ticks)
        time.sleep(0.5)


def request_worker_ticks(worker_cmd_queues):
    for q in worker_cmd_queues:
        q.put({"action": "get_tick"})


def get_worker_ticks(worker_resp_queues):
    ticks = {}
    for i, resp_q in enumerate(worker_resp_queues):
            resp = resp_q.get()
            ticks[resp["broker"]] = resp["tick"]
    return ticks
