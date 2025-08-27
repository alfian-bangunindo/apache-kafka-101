from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, max, min, round, window
from pyspark.sql.streaming import StreamingQuery

from src.data_transformer.base_transformer import BaseTransformer


class AggregationTransformer(BaseTransformer):
    def write_to_db(self, df: DataFrame) -> StreamingQuery:
        return super().write_to_db(df)

    def write_to_kafka(self, df: DataFrame) -> StreamingQuery:
        return super().write_to_kafka(df)

    def process(self, df: DataFrame, sink: str):
        return super().process(df, sink)

    def transform(self, df):
        agg_df = (
            df.withWatermark("event_time", "10 seconds")
            .groupBy(
                "device_id",
                window("event_time", "2 minutes"),
            )
            .agg(
                round(avg("temperature_value"), 2).alias("avg_temperature"),
                max("temperature_value").alias("max_temperature"),
                min("temperature_value").alias("min_temperature"),
                round(avg("humidity_value"), 2).alias("avg_humidity"),
                round(avg("wind_speed_value"), 2).alias("avg_wind_speed"),
                max("uv_index").alias("max_uv_index"),
            )
        )
        agg_df = (
            agg_df.withColumn("window_start", col("window.start"))
            .withColumn("window_end", col("window.end"))
            .drop("window")
        )

        return agg_df
