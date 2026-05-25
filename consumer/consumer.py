"""
This is the consumer component of the weather data pipeline. 
It consumes messages from the Kafka topic 'weather-stream' and stores the data in a Cassandra database.
The consumer connects to Kafka, listens for incoming weather data messages, and upon receiving a message,
it deserializes the JSON data and inserts it into the 'weather_data' table in Cassandra.
The table is structured to store the city, timestamp, temperature, humidity, and weather description.
The consumer runs indefinitely, processing messages as they arrive and logging the inserted data to the console.
"""
import json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from datetime import datetime

consumer = KafkaConsumer(
    'weather-stream',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

cluster = Cluster(['cassandra'])
session = cluster.connect()

session.execute("""
CREATE KEYSPACE IF NOT EXISTS weather
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
""")

session.set_keyspace('weather')

session.execute("""
CREATE TABLE IF NOT EXISTS weather_data (
    city TEXT,
    timestamp TIMESTAMP,
    temperature FLOAT,
    humidity INT,
    weather TEXT,
    PRIMARY KEY (city, timestamp)
)
""")

insert_query = session.prepare("""
INSERT INTO weather_data (
    city,
    timestamp,
    temperature,
    humidity,
    weather
)
VALUES (?, ?, ?, ?, ?)
""")

print("Consumer started...")

for message in consumer:
    data = message.value

    session.execute(insert_query, (
        data['city'],
        datetime.fromtimestamp(data['timestamp'] / 1000),
        data['temperature'],
        data['humidity'],
        data['weather']
    ))

    print(f"Inserted: {data}")