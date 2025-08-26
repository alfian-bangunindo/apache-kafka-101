import random
import time
from argparse import ArgumentParser

from pyspark.sql import SparkSession

from src.config import JSON_SCHEMA, NUM_DEVICES
from src.data_generator.weather_station import DummyDataWeatherStation
from src.data_streamer.consumer import SparkConsumer
from src.data_streamer.producer import KafkaProducerWrapper
from src.data_transformer.aggregation_transformer import AggregationTransformer
from src.data_transformer.flatten_transformer import FlattenJSONTransformer

if __name__ == "__main__":
    parser = ArgumentParser(
        description="Simulate streaming processing of weather station data to Kafka topic and database."
    )
    parser.add_argument("--role")
    args = parser.parse_args()

    if args.role == "produce_raw_data":
        # Produce raw data to Kafka topic
        producer = KafkaProducerWrapper()
        data_generator = DummyDataWeatherStation(NUM_DEVICES)
        try:
            while True:
                data = data_generator.next_data_stream()
                producer.send(data)
                time.sleep(random.uniform(1, 2))
                print("\n")

        except KeyboardInterrupt:
            print("Data streaming is done!")

        finally:
            producer.close()

    elif args.role == "transform_data":
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
            .master("local[*]")
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

    else:
        print("Please provide a valid role: produce_raw_data or transform_data")
