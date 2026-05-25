# Dockerfile for the producer service

# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install any needed packages specified in requirements.txt
COPY requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the producer script into the container
COPY producer.py .

# Command to run the producer script when the container starts 
CMD ["python", "producer.py"] 