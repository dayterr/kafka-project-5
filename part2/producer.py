import json
import logging
from random import randint
import time

from confluent_kafka import Producer

WORDS = ['rabbit', 'sun', 'sea', 'grime', 'bass']


def delivery_report(err, msg):
    if err is not None:
        logging.error(f'error when sending: {err}')
    else:
        logging.info('message sent to the topic')


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

conf = {
    "bootstrap.servers": "127.0.0.1:9093",
    "acks": "all",
    "retries": 5, 
} 

logger.info('producer started')

producer = Producer(conf)

while True:
    ind = randint(0, 4)
    message = WORDS[ind]
    #producer.produce(topic='nifi-topic', value=json.dumps({'msg': message}), callback=delivery_report)
    producer.produce(topic='nifi-raw', value=json.dumps({'msg': message}), callback=delivery_report)
    producer.flush()
    logger.info(f'message {message} sent')
    time.sleep(5)
