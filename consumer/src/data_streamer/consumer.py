from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, from_json
from pyspark.sql.udf import StructType
from src.data_transformer.base_spark_wrapper import BaseSparkWrapper


class SparkConsumer(BaseSparkWrapper):
    """
    A class to consume data from Kafka using Spark Structured Streaming.
    """

    def read_from_kafka(
        self,
        json_schema: StructType,
    ) -> DataFrame:
        """
        Read data from Kafka topic and parse the JSON messages.
        """
        # Read message from kafka
        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .option("subscribe", self.kafka_topic_source)
            .option("startingOffsets", "earliest")
            .load()
        )
        df = df.withColumn("value", expr("cast(value as string)"))

        # Only take value key from message
        df = df.withColumn(
            "values_json", from_json(col("value"), json_schema)
        ).selectExpr("values_json.*")

        return df
