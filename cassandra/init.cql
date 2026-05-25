CREATE KEYSPACE IF NOT EXISTS weather
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE weather;

CREATE TABLE IF NOT EXISTS weather_data (
    city TEXT,
    timestamp TIMESTAMP,
    temperature FLOAT,
    humidity INT,
    weather TEXT,
    PRIMARY KEY (city, timestamp)
);