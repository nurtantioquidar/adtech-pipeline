import os
from pyspark.sql import SparkSession


def get_spark_session(app_name):
    """
    Creates and returns a SparkSession.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


def get_kafka_bootstrap_servers():
    """
    Returns Kafka bootstrap servers from an environment variable or a default.
    """
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
