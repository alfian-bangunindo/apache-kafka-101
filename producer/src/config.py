import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_SOURCE = os.getenv("KAFKA_TOPIC_SOURCE")

NUM_DEVICES = 6
