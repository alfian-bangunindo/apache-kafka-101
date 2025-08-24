import random
import time
from argparse import ArgumentParser

from src.config import NUM_DEVICES
from src.data_generator.weather_station import DummyDataWeatherStation
from src.data_streamer.consumer import KafkaConcumerWrapper
from src.data_streamer.database_handler import DatabaseHandler
from src.data_streamer.producer import KafkaProducerWrapper
from src.utils import transform_weather_data

if __name__ == "__main__":
    parser = ArgumentParser(
        description="Simulate streaming data from weather station devices to Kafka topic."
    )
    parser.add_argument("--role")
    args = parser.parse_args()

    if args.role == "produce_raw_data":
        raw_data_producer = KafkaProducerWrapper()
        data_generator = DummyDataWeatherStation(NUM_DEVICES)
        try:
            while True:
                data = data_generator.next_data_stream()
                raw_data_producer.send(data)
                time.sleep(random.uniform(1, 2))
                print("\n")

        except KeyboardInterrupt:
            print("Data streaming is done!")

        finally:
            raw_data_producer.close()

    elif args.role == "consume_raw_data":
        raw_data_consumer = KafkaConcumerWrapper(transform_weather_data)
        transform_data_producer = KafkaProducerWrapper(
            topic_name="weather_station_transformed"
        )
        db_handler = DatabaseHandler()
        try:
            for data in raw_data_consumer.consume():
                transform_data_producer.send(data)
                db_handler.insert_data(data)

                print("\n")

        except KeyboardInterrupt:
            print("Data consuming is done!")

    else:
        print("Please provide a valid role: produce_raw_data or consume_raw_data")
