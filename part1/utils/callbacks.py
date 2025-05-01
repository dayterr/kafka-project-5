import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Delivery failed: {err}")
    else:
        logging.info(f"Delivered message to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
