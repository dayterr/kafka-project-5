from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from utils.callbacks import delivery_report
from utils.constants import MESSAGE_SCHEMA_STR
from utils.serializers import message_to_dict

if __name__ == "__main__":
    bootstrap_servers = "rc1a-2it2mhmvlhsb2cqp.mdb.yandexcloud.net:9091,rc1b-pvdblahn1i73gsfh.mdb.yandexcloud.net:9091,rc1d-0hc2kaknrrd2a4f3.mdb.yandexcloud.net:9091"
    schema_registry_url = "https://srnpb4efj109aer6luqh.schema-registry.yandexcloud.net:443"
    topic = "test-message"
    subject = "messageschema"

    producer_conf = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": "/Users/ruthdayter/Dev/study/kafka/kafka-project-5/part1/YandexInternalRootCA.crt",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "admin",
        "sasl.password": "adminpass",
        }

    producer = Producer(producer_conf)
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url, "basic.auth.user.info": "api-key:AQVN0TJw4i5dC-NKXePu73tpwb8Kvlv39tReYQj5"})

    json_serializer = JSONSerializer(MESSAGE_SCHEMA_STR,
                                     schema_registry_client,
                                     message_to_dict)
    message = {
        "text": "hey there, Kafka",
        "somenumber": 1,
        "color": "green",
    }
    context = SerializationContext(topic, MessageField.VALUE)
    serialized_value = json_serializer(message, context)

    while True:
        producer.produce(
            topic,
            key="first-message",
            value=serialized_value,
            headers=[("myTestHeader", b"header values are binary")],
            callback=delivery_report,
            )
        producer.flush()
