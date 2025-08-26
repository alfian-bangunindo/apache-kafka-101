from pyspark.sql import SparkSession

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_SOURCE, KAFKA_TOPIC_TARGET


class BaseSparkWrapper:
    def __init__(
        self,
        spark: SparkSession,
        kafka_bootstrap_servers: str | None = KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic_source: str | None = KAFKA_TOPIC_SOURCE,
        kafka_topic_target: str | None = KAFKA_TOPIC_TARGET,
    ):
        self.spark = spark

        if not kafka_bootstrap_servers:
            raise ValueError(
                "KAFKA_BOOTSTRAP_SERVERS is not set in environment variables."
            )
        self.kafka_bootstrap_servers = kafka_bootstrap_servers

        if not kafka_topic_source:
            raise ValueError("KAFKA_TOPIC_SOURCE is not set in environment variables.")
        self.kafka_topic_source = kafka_topic_source

        self.kafka_topic_target = kafka_topic_target
