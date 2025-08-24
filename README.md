# Apache Kafka 101 - Weather Station Data Pipeline

## Project Structure

```
apache-kafka-101/
├── src/
│   ├── config.py              # Configuration variables
│   ├── utils.py               # Utility functions
│   ├── data_generator/         # Weather data generation
│   └── data_streamer/        # Kafka producer/consumer
├── init-scripts/              # Database initialization
├── docker-compose.yaml     # Docker services
├── requirements.txt        # Python dependencies
└── main.py                # Entry point
```

## Setup

### Installation

1. Clone the repository.

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create `.env` file:

```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Start doker compose:

```bash
docker compose up -d
```

This command will run both Kafka server and Kafka UI. It also initialize database with table called `weather_sensor_readings`

## How to Run

You need to open two terminal in order to see both log of producer and consumer.

### Start Producer (Generate & Send Data)

```bash
python main.py --role produce_raw_data
```

### Start Consumer (Transform, Push to New Topic, and Store Data to DB)

```bash
python main.py --role consume_raw_data
```

## Sample Output

### Producer Output

```
STARTING KAFKA PRODUCER...
'topic_name' is not provided, using KAFKA_TOPIC from environment variables.
Connection to Kafka broker successful.
==================================================
[2025-08-24 19:57:30.191] weather-station-5 | Location: (-7.158862, 112.849642) | Temp: 28.81°C | Humidity: 56% | Wind: 5.21 km/h South-West | Precipitation: 0 mm | UV Index: 8.44
Data sent to Kafka topic: weather_station_data


[2025-08-24 19:57:32.094] weather-station-4 | Location: (-7.318575, 112.780479) | Temp: 31.81°C | Humidity: 67% | Wind: 9.57 km/h South | Precipitation: 0 mm | UV Index: 9.13
Data sent to Kafka topic: weather_station_data


[2025-08-24 19:57:33.706] weather-station-2 | Location: (-7.420383, 112.64485) | Temp: 29.65°C | Humidity: 51% | Wind: 5.59 km/h South-West | Precipitation: 9.2 mm | UV Index: 0.59
Data sent to Kafka topic: weather_station_data

...
```

### Consumer Output

```
STARTING KAFKA CONSUMER...
'topic_name' is not provided, using KAFKA_TOPIC from environment variables.
'consumer_group' is not provided, using KAFKA_CONSUMER_GROUP from environment variables.
Connection to Kafka broker successful.
==================================================
STARTING KAFKA PRODUCER...
Connection to Kafka broker successful.
==================================================
Data received from Kafka topic: weather_station_data
Data sent to Kafka topic: weather_station_transformed
Inserted data for device weather-station-5


Data received from Kafka topic: weather_station_data
Data sent to Kafka topic: weather_station_transformed
Inserted data for device weather-station-4


Data received from Kafka topic: weather_station_data
Data sent to Kafka topic: weather_station_transformed
Inserted data for device weather-station-2

...
```

### Kafka Topic: `weather_station_data` (Store Raw Data)

![weather_station_data](./images/weather_station_raw_data.png)

### Kafka Topic: `weather_station_transformed` (Store Transformed Data)

![weather_station_transformedr](./images/weather_station_transformed_data.png)

### Database Query Results

````sql
SELECT * FROM weather_sensor_readings LIMIT 10;
```a

Output:

````

event_id | device_id | timestamp | latitude | longitude | temperature | humidity | wind_speed | wind_direction | precipitation_mm | uv_index | uv_level
----------+-------------------+-------------------------+-----------+------------+-------------+----------+------------+----------------+------------------+----------+-----------
1 | weather-station-5 | 2025-08-24 19:57:30.191 | -7.158862 | 112.849642 | 28.81 | 56 | 5.21 | South-West | 0 | 8.44 | Very High
2 | weather-station-4 | 2025-08-24 19:57:32.094 | -7.318575 | 112.780479 | 31.81 | 67 | 9.57 | South | 0 | 9.13 | Very High
3 | weather-station-2 | 2025-08-24 19:57:33.706 | -7.420383 | 112.64485 | 29.65 | 51 | 5.59 | South-West | 9.2 | 0.59 | Low
4 | weather-station-5 | 2025-08-24 19:57:35.637 | -7.158862 | 112.849642 | 28.79 | 56 | 5.2 | South-West | 0 | 8.44 | Very High
5 | weather-station-1 | 2025-08-24 19:57:37.025 | -7.339019 | 112.841093 | 30.34 | 74 | 6.81 | North-West | 5.99 | 0.57 | Low
6 | weather-station-3 | 2025-08-24 19:57:38.332 | -7.156054 | 112.740006 | 27.52 | 77 | 7.59 | South | 6.45 | 0.25 | Low
7 | weather-station-6 | 2025-08-24 19:57:39.678 | -7.315009 | 112.746605 | 31.38 | 51 | 9.06 | North | 0 | 9.06 | Very High
8 | weather-station-5 | 2025-08-24 19:57:41.583 | -7.158862 | 112.849642 | 28.79 | 56 | 5.2 | South-West | 0 | 8.43 | Very High
9 | weather-station-1 | 2025-08-24 19:57:42.894 | -7.339019 | 112.841093 | 30.34 | 74 | 6.82 | North-West | 5.99 | 0.57 | Low
10 | weather-station-1 | 2025-08-24 19:57:43.981 | -7.339019 | 112.841093 | 30.35 | 74 | 6.83 | North-West | 5.99 | 0.61 | Low

```

```

