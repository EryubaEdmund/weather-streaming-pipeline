# Dockerfile for the consumer component of the weather data pipeline. 
# This Dockerfile sets up a Python environment, installs necessary dependencies, 
# and runs the consumer script that listens to the Kafka topic and stores data in Cassandra.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

CMD ["python", "consumer.py"]