import json

from kafka import KafkaProducer

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


class KafkaProducerWrapper:
    """
    A wrapper class for Kafka producer to send messages to a Kafka topic.
    """

    def __init__(self, topic_name: str | None = None):
        """
        Initialize the Kafka producer with the provided or environment variable configurations.

        Args:
            topic_name (str, optional): The Kafka topic name to consume from. If None, uses KAFKA_TOPIC from environment variables. Defaults to None.
        """
        print("STARTING KAFKA PRODUCER...")

        if KAFKA_BOOTSTRAP_SERVERS:
            self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        else:
            raise ValueError(
                "KAFKA_BOOTSTRAP_SERVERS is not set in environment variables."
            )

        if topic_name is not None:
            self.topic_name = topic_name
        else:
            print(
                "'topic_name' is not provided, using KAFKA_TOPIC from environment variables."
            )
            if KAFKA_TOPIC is None:
                raise ValueError("KAFKA_TOPIC is not set in environment variables.")
            self.topic_name = KAFKA_TOPIC

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        print("Connection to Kafka broker successful.")
        print("=" * 50)

    def send(self, data: dict):
        """
        Send data to the Kafka topic.
        """
        self.producer.send(self.topic_name, value=data)

        print("Data sent to Kafka topic:", self.topic_name)

    def close(self):
        """
        Close the Kafka producer connection.
        """
        self.producer.flush()
        self.producer.close()

        print("Kafka producer connection closed.")
