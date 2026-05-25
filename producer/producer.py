"""
Weather Producer for Kafka
This script fetches weather data for a specified city from the OpenWeatherMap API and sends it to a Kafka topic named "weather-stream". 
It includes retry logic for connecting to Kafka and handles potential exceptions during data fetching and message sending.
Requirements:
- An OpenWeatherMap API key set as an environment variable (OPENWEATHER_API_KEY).
- Kafka running and accessible at 'kafka:9092'.
"""

import json
import time
import os
import requests

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Load the OpenWeatherMap API key from environment variables
API_KEY = os.getenv("OPENWEATHER_API_KEY")

CITY = "Nairobi"
URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

TOPIC = "weather-stream"

MAX_RETRIES = 15
RETRY_DELAY = 5  # seconds


def create_producer():
    """Create and return a KafkaProducer instance with retry logic.

    Tries to connect up to `MAX_RETRIES` times, sleeping `RETRY_DELAY`
    seconds between attempts. The producer serializes values as JSON.
    Raises a RuntimeError if all attempts fail.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Informational log about connection attempts
            print(f"[Producer] Connecting to Kafka (attempt {attempt}/{MAX_RETRIES})...")

            # Create Kafka producer with JSON value serialization
            p = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
            )

            print("[Producer] Connected to Kafka successfully.")
            return p
        except NoBrokersAvailable:
            # Broker not available yet; wait and retry
            print(f"[Producer] Broker not available. Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    # If we fall out of the loop, all retries failed
    raise RuntimeError("Could not connect to Kafka after maximum retries.")


def fetch_weather():
    """Fetch current weather data from OpenWeatherMap API.

    Returns a dict with a small subset of fields (city, temperature,
    humidity, weather description, and timestamp in ms). On failure,
    logs the exception and returns None so the caller can skip sending.
    """
    try:
        # Request weather data with a short timeout to avoid blocking
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Map only the fields we care about into a compact payload
        return {
            "city": data["name"],
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["description"],
            # Use milliseconds since epoch for easier time-series indexing
            "timestamp": int(time.time() * 1000)
        }
    except Exception as e:
        # Non-fatal: print and return None so producer loop can continue
        print(f"[Producer] Failed to fetch weather data: {e}")
        return None


# Initialize the Kafka producer (may block while retrying)
producer = create_producer()


def main_loop(p):
    """Main publishing loop: fetch weather and send to Kafka repeatedly.

    The loop sleeps for 10 seconds between iterations. If fetching fails,
    the loop simply waits and retries on the next iteration. If sending
    fails, the error is logged and the loop continues.
    """
    while True:
        weather_data = fetch_weather()

        if weather_data:
            try:
                # Send the JSON-serialized message to the configured topic
                p.send(TOPIC, weather_data)
                # Block until all buffered messages are flushed to broker
                p.flush()
                print(f"[Producer] Sent: {weather_data}")
            except Exception as e:
                # Log send failures; do not crash the loop
                print(f"[Producer] Failed to send message: {e}")

        # Throttle the loop to avoid spamming the API
        time.sleep(10)


if __name__ == "__main__":
    # Start the main loop when run as a script
    main_loop(producer)