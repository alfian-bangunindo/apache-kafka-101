from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from src.data_transformer.base_spark_wrapper import BaseSparkWrapper
from src.utils import insert_spark_df_to_db


class BaseTransformer(ABC, BaseSparkWrapper):
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    def process(self, df: DataFrame, output: str):
        transformed_df = self.transform(df)

        if output == "kafka":
            return self.write_to_kafka(transformed_df)
        elif output == "db":
            return self.write_to_db(transformed_df)
        else:
            raise ValueError("output must be either 'kafka' or 'db'")

    def write_to_kafka(self, df: DataFrame) -> StreamingQuery:
        # Check if kafka_topic_target is set
        if not self.kafka_topic_target:
            raise ValueError("kafka_topic_target is not set")

        # Convert data to json and make alias as value
        df = df.selectExpr("to_json(struct(*)) AS value")

        # Streaming the data to kafka topic
        return (
            df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("topic", self.kafka_topic_target)
            .option("checkpointLocation", "checkpoint_kafka_topic")
            .outputMode("append")
            .start()
        )

    def write_to_db(self, df: DataFrame) -> StreamingQuery:
        # Streaming the data to db
        return (
            df.writeStream.foreachBatch(insert_spark_df_to_db)
            .trigger(processingTime="10 seconds")
            .option("checkpointLocation", "checkpoint_db")
            .start()
        )
