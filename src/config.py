import os

from dotenv import load_dotenv
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_SOURCE = os.getenv("KAFKA_TOPIC_SOURCE")
KAFKA_TOPIC_TARGET = os.getenv("KAFKA_TOPIC_TARGET")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

NUM_DEVICES = 6

JSON_SCHEMA = StructType(
    [
        StructField(
            "device",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField(
                        "location",
                        StructType(
                            [
                                StructField("latitude", DoubleType(), True),
                                StructField("longitude", DoubleType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("timestamp", TimestampType(), True),
        StructField(
            "sensors",
            StructType(
                [
                    StructField(
                        "environment",
                        StructType(
                            [
                                StructField(
                                    "temperature",
                                    StructType(
                                        [
                                            StructField("value", DoubleType(), True),
                                            StructField("unit", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                                StructField(
                                    "humidity",
                                    StructType(
                                        [
                                            StructField("value", DoubleType(), True),
                                            StructField("unit", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "weather",
                        StructType(
                            [
                                StructField(
                                    "wind",
                                    StructType(
                                        [
                                            StructField(
                                                "speed",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "value", DoubleType(), True
                                                        ),
                                                        StructField(
                                                            "unit", StringType(), True
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            StructField(
                                                "direction", StringType(), True
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                StructField(
                                    "precipitation",
                                    StructType(
                                        [
                                            StructField("value", DoubleType(), True),
                                            StructField("unit", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                                StructField(
                                    "uv_index",
                                    StructType(
                                        [
                                            StructField("value", DoubleType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)
