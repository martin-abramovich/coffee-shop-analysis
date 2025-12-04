import logging
import os

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

def init_log(log_name: str = __name__):

    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s'
    )

    logging.getLogger("pika").setLevel(logging.ERROR)

    return logging.getLogger(log_name)