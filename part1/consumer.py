from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

from utils.serializers import message_from_dict


if __name__ == "__main__":
    schema_registry_url = "https://srnpb4efj109aer6luqh.schema-registry.yandexcloud.net:443"
    topic = "test-message"

    consumer_conf = {
        "bootstrap.servers": "rc1a-2it2mhmvlhsb2cqp.mdb.yandexcloud.net:9091,rc1b-pvdblahn1i73gsfh.mdb.yandexcloud.net:9091,rc1d-0hc2kaknrrd2a4f3.mdb.yandexcloud.net:9091",
        "security.protocol": "SASL_SSL",
        "group.id": "group",
        "auto.offset.reset": "earliest",
        "ssl.ca.location": "/Users/ruthdayter/Dev/study/kafka/kafka-project-5/part1/YandexInternalRootCA.crt",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "admin",
        "sasl.password": "adminpass",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url, "basic.auth.user.info": "api-key:AQVN0TJw4i5dC-NKXePu73tpwb8Kvlv39tReYQj5"})
    latest = schema_registry_client.get_latest_version(topic + "-value")
    json_schema_str = latest.schema.schema_str
    json_deserializer = JSONDeserializer(json_schema_str, from_dict=message_from_dict)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            context = SerializationContext(msg.topic(), MessageField.VALUE)
            message = json_deserializer(msg.value(), context)
            print(f"Message on {msg.topic()}:\n{message}")
            if msg.headers():
                print(f"Headers: {msg.headers()}")
    except KeyboardInterrupt as err:
        print(f"Consumer error: {err}")
    finally:
        consumer.close()
        print("Closing consumer")
