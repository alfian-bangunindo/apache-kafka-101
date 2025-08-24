CREATE TABLE IF NOT EXISTS weather_sensor_readings (
    event_id SERIAL PRIMARY KEY,
    device_id VARCHAR(50),
    timestamp TIMESTAMP NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    temperature FLOAT NOT NULL,
    humidity INT NOT NULL,
    wind_speed FLOAT NOT NULL,
    wind_direction VARCHAR(30) NOT NULL,
    precipitation_mm FLOAT NOT NULL,
    uv_index FLOAT NOT NULL,
    uv_level VARCHAR(20) NOT NULL
);
