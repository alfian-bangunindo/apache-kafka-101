from pyspark.sql.functions import col

from src.data_transformer.base_transformer import BaseTransformer


class FlattenJSONTransformer(BaseTransformer):
    def transform(self, df):
        # Flatten the JSON structure
        flattened_df = df.select(
            col("device.id").alias("device_id"),
            col("device.location.latitude").alias("latitude"),
            col("device.location.longitude").alias("longitude"),
            col("timestamp").alias("event_time"),
            # Environment
            col("sensors.environment.temperature.value").alias("temperature_value"),
            col("sensors.environment.humidity.value").alias("humidity_value"),
            # Weather
            col("sensors.weather.wind.speed.value").alias("wind_speed_value"),
            col("sensors.weather.wind.direction").alias("wind_direction"),
            col("sensors.weather.precipitation.value").alias("precipitation_value"),
            col("sensors.weather.uv_index.value").alias("uv_index"),
        )
        return flattened_df
