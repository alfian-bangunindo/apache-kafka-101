import json

from kafka import KafkaProducer

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_SOURCE


class KafkaProducerWrapper:
    """
    A wrapper class for Kafka producer to send messages to a Kafka topic.
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str | None = KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic_source: str | None = KAFKA_TOPIC_SOURCE,
    ):
        """
        Initialize the Kafka producer with the provided or environment variable configurations.

        Args:
            kafka_topic_source (str, optional): The Kafka topic name to consume from. If None, uses KAFKA_TOPIC from environment variables. Defaults to None.
        """
        print("STARTING KAFKA PRODUCER...")

        if not kafka_bootstrap_servers:
            raise ValueError(
                "KAFKA_BOOTSTRAP_SERVERS is not set in environment variables."
            )
        self.kafka_bootstrap_servers = kafka_bootstrap_servers

        if not kafka_topic_source:
            raise ValueError("KAFKA_TOPIC_SOURCE is not set in environment variables.")
        self.kafka_topic_source = kafka_topic_source

        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        print("Connection to Kafka broker successful.")
        print("=" * 50)

    def send(self, data: dict):
        """
        Send data to the Kafka topic.
        """
        self.producer.send(self.kafka_topic_source, value=data)

        print("Data sent to Kafka topic:", self.kafka_topic_source)

    def close(self):
        """
        Close the Kafka producer connection.
        """
        self.producer.flush()
        self.producer.close()

        print("Kafka producer connection closed.")
