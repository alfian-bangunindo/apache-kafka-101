CREATE TABLE IF NOT EXISTS weather_sensor_aggregate (
    device_id VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_temperature FLOAT NOT NULL,
    min_temperature FLOAT NOT NULL,
    max_temperature FLOAT NOT NULL,
    avg_humidity INT NOT NULL,
    avg_wind_speed FLOAT NOT NULL,
    max_uv_index FLOAT NOT NULL
);

ALTER TABLE
    weather_sensor_aggregate
ADD
    PRIMARY KEY (device_id, window_start, window_end);
