# Use a Debian-based image with Java (for PySpark compatibility)
FROM openjdk:8-jdk-slim

# Install Python3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt /tmp/
RUN pip3 install --upgrade pip && \
    pip3 install -r /tmp/requirements.txt

# Set the working directory
WORKDIR /app

# Copy the application code (assumes your local code is in the 'app' folder)
COPY app/ .

# **Set PySpark submit args to include the Kafka package**
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 pyspark-shell"

# Set the command to run your main entry point
CMD ["python3", "main.py"]
