import logging

from confluent_kafka import Consumer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.info("consumer started")


if __name__ == "__main__":
    consumer_conf = {
        "bootstrap.servers": "localhost:9093",
        "group.id": "nifi-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 6_000,
        }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(["nifi-topic"])
    try:
        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Ошибка: {msg.error()}")
                continue
            value = msg.value().decode("utf-8")
            logging.info(
                f"Получено сообщение: {value=}, "
                f"partition={msg.partition()}, offset={msg.offset()}"
                )
    finally:
        consumer.close()
