from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from utils.callbacks import delivery_report
from utils.constants import MESSAGE_SCHEMA_STR
from utils.serializers import message_to_dict

if __name__ == "__main__":
    bootstrap_servers = "rc1a-2it2mhmvlhsb2cqp.mdb.yandexcloud.net,rc1b-pvdblahn1i73gsfh.mdb.yandexcloud.net,rc1d-0hc2kaknrrd2a4f3.mdb.yandexcloud.net"
    schema_registry_url = "https://srnpb4efj109aer6luqh.schema-registry.yandexcloud.net:443/subjects"
    topic = "test-topic"
    subject = topic + "-value"

    producer_conf = {
        "bootstrap.servers": bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'ssl.ca.location': 'YandexInternalRootCA.crt',
        'sasl.mechanism': 'SCRAM-SHA-512',
        "sasl.username": "admin",
        "sasl.password": "adminpass",
        #"api-key": "AQVNwaPNotPrCTAj7xZlOgl-KojEHD_L8D1Gy5aL"
        }
    producer = Producer(producer_conf)
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url, "basic.auth.user.info": "api-key:AQVNwaPNotPrCTAj7xZlOgl-KojEHD_L8D1Gy5aL"})

    try:
        latest = schema_registry_client.get_latest_version(subject)
        print(f"Schema is already registered for {subject}:\n{latest.schema.schema_str}")
    except Exception:
        schema_object = Schema(MESSAGE_SCHEMA_STR, "JSON")
        schema_id = schema_registry_client.register_schema(
            subject,
            schema_object)
        print(f"Registered schema for {subject} with id: {schema_id}")

    json_serializer = JSONSerializer(MESSAGE_SCHEMA_STR,
                                     schema_registry_client,
                                     message_to_dict)
    message = {
        "test": "hey there, Kafka",
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
