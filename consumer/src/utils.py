from pyspark.sql import DataFrame
from src.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


def insert_spark_df_to_db(df: DataFrame, batch_id: int):
    print(f"Batch ID: {batch_id}")
    try:
        (
            df.write.mode("append")
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option(
                "url",
                f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
            )
            .option("dbtable", "weather_sensor_aggregate")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .save()
        )

        df.show(truncate=False)

        print("Data inserted into PostgreSQL successfully.")

    except Exception as e:
        print(f"Failed to insert data into PostgreSQL: {e}")
