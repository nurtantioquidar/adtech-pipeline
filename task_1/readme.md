# Conversion Attribution Pipeline

This project implements a conversion attribution pipeline using PySpark Structured Streaming. It ingests click and conversion events from Kafka, performs stream-to-stream joins, deduplicates events, aggregates campaign metrics, and writes the output to a sink (in our demo, the console).

## Overview

The pipeline is designed to:

- Ingest streaming click and conversion events from Kafka.
- Perform stream-stream joins (using Append mode) within a configurable attribution window.
- Deduplicate and validate events.
- Aggregate campaign performance metrics.
- Ensure integration with Kafka using the appropriate Spark SQL Kafka package.

## Architecture
- Data Sources: Mobile SDKs and web pixels emit click and conversion events.
- Kafka Broker: Acts as a message bus to ingest and buffer the high-volume events.
- PySpark Application: Reads from Kafka, applies watermarking, deduplicates events, performs stream-stream joins (in Append mode), and aggregates metrics.
- Sink: Aggregated metrics are written to a persistent store or, for demo purposes, to the console.

## Directory Structure
```
task_1/
├── app
│   ├── main.py           # Main entry point that triggers the pipeline
│   ├── pipeline.py       # Contains the PySpark pipeline logic (ingestion, join, aggregation)
│   └── utils.py          # Helper functions (e.g., Spark session and Kafka config)
├── tests
│   └── integration_test.py  # Integration tests for the pipeline (optional)
├── Dockerfile            # Dockerfile to build the PySpark application image
├── docker-compose.yml    # Docker Compose file to deploy the pipeline (with Kafka and the PySpark app)
└── requirements.txt      # Python dependencies (including pyspark)
```

## Components
- main.py:
Calls the run_pipeline() function from pipeline.py.
- pipeline.py:
Defines the PySpark Structured Streaming job, which:
    - Reads click and conversion data from Kafka.
    - Applies watermarking and deduplication.
    - Joins streams based on user ID within a 30-minute window.
    - Aggregates campaign metrics using 5‑minute windows.
    - Writes output in Append mode (required for stream-stream joins).
- utils.py:
Contains helper functions for creating the Spark session and fetching environment-specific configurations such as Kafka bootstrap servers.

## Build and Run
```
docker-compose up --build
```

## Limitation
- Not working yet due to Kafka not being set