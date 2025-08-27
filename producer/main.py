import random
import time
from argparse import ArgumentParser

from src.config import NUM_DEVICES
from src.data_generator.weather_station import DummyDataWeatherStation
from src.data_streamer.producer import KafkaProducerWrapper

if __name__ == "__main__":
    parser = ArgumentParser(
        description="Producer application to generate dummy weather station data and send it to Kafka topic"
    )

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
