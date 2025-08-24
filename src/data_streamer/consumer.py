import json

from kafka import KafkaConsumer

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC


class KafkaConcumerWrapper:
    def __init__(
        self,
        transform_func=None,
        topic_name: str | None = None,
        consumer_group: str | None = None,
    ):
        print("STARTING KAFKA CONSUMER...")
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

        if consumer_group is not None:
            self.consumer_group = consumer_group
        else:
            print(
                "'consumer_group' is not provided, using KAFKA_CONSUMER_GROUP from environment variables."
            )
            if KAFKA_CONSUMER_GROUP is None:
                raise ValueError(
                    "KAFKA_CONSUMER_GROUP is not set in environment variables."
                )
            self.consumer_group = KAFKA_CONSUMER_GROUP

        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.consumer_group,
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        self.transform_func = transform_func or (lambda msg: msg)
        print("Connection to Kafka broker successful.")
        print("=" * 50)

    def consume(self):
        try:
            for message in self.consumer:
                data = self.transform_func(message.value)
                print("Data received from Kafka topic:", self.topic_name)
                yield data
        finally:
            self.consumer.close()
            print("Kafka consumer connection closed.")
