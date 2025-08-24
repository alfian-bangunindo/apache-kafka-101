import psycopg2

from src.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


class DatabaseHandler:
    """
    A class to handle PostgreSQL database operations.
    """

    def __init__(self):
        """
        Initialize the database connection using environment variable configurations.
        """
        self.conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
        )

    def insert_data(self, data: dict):
        """
        Insert data into the weather_sensor_readings table.
        Args:
            data (dict): A dictionary containing the device ID, timestamp, location, and sensor readings.
        """
        insert_query = """
            INSERT INTO weather_sensor_readings
            (
                device_id,
                timestamp,
                latitude,
                longitude, 
                temperature, 
                humidity, 
                wind_speed, 
                wind_direction,
                precipitation_mm,
                uv_index,
                uv_level
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self.conn.cursor() as cursor:
            cursor.execute(
                insert_query,
                (
                    data["device_id"],
                    data["timestamp"],
                    data["latitude"],
                    data["longitude"],
                    data["temperature"],
                    data["humidity"],
                    data["wind_speed"],
                    data["wind_direction"],
                    data["precipitation_mm"],
                    data["uv_index"],
                    data["uv_level"],
                ),
            )
            self.conn.commit()

            print(f"Inserted data for device {data['device_id']}")

    def close(self):
        """
        Close the database connection.
        """
        self.conn.close()

        print("Database connection closed.")
