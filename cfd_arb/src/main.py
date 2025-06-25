import logging
import sys
import time

def main():
    pass


def setup_logger() -> logging.Logger:
    """
    Set up a logger that only writes to stdout, suitable for long-running trading bots on a VPS.
    """
    logger = logging.getLogger("arbitrage_bot")
    logger.setLevel(logging.INFO)

    # Remove any existing handlers (reload safety)
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        '[%(asctime)s UTC] %(levelname)s %(name)s (%(threadName)s): %(message)s'
    )
    logging.Formatter.converter = time.gmtime

    # Stream handler for stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

    return logger


if __name__ == "__main__":
    setup_logger()
    main()
