from argparse import ArgumentParser

from pyspark.sql import SparkSession
from src.config import JSON_SCHEMA, SPARK_MASTER_URL
from src.data_streamer.consumer import SparkConsumer
from src.data_transformer.aggregation_transformer import AggregationTransformer
from src.data_transformer.flatten_transformer import FlattenJSONTransformer

if __name__ == "__main__":
    parser = ArgumentParser(
        description="Consumer application to read weather station data from Kafka, process it with Spark, and store results in PostgreSQL"
    )

    # Initialize spark session
    spark = (
        SparkSession.builder.appName("Weather Station Data Streaming")
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config("spark.sql.shuffle.partitions", 4)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "org.postgresql:postgresql:42.2.20",
        )
        .master(SPARK_MASTER_URL)
        .getOrCreate()
    )

    # Consume data from kafka topic using spark
    consumer = SparkConsumer(spark)
    raw_df = consumer.read_from_kafka(JSON_SCHEMA)

    # Flatten raw dataframe
    flatten_transformer = FlattenJSONTransformer(spark)
    flatten_query = flatten_transformer.process(raw_df, "kafka")

    # Aggregate flattened dataframe
    flattened_df = flatten_transformer.transform(raw_df)
    aggregate_transformer = AggregationTransformer(spark)
    agg_query = aggregate_transformer.process(flattened_df, "db")

    spark.streams.awaitAnyTermination()
