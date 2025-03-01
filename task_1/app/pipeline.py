from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import utils


def run_pipeline():
    spark = utils.get_spark_session("ConversionAttributionPipeline")

    # Define schemas for click and conversion events
    clickSchema = StructType(
        [
            StructField("userId", StringType(), True),
            StructField("campaignId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("eventId", StringType(), True),
        ]
    )

    conversionSchema = StructType(
        [
            StructField("userId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("eventId", StringType(), True),
        ]
    )

    # Kafka configuration (provided via utils or environment variables)
    kafka_bootstrap_servers = utils.get_kafka_bootstrap_servers()
    click_topic = "click-topic"
    conversion_topic = "conversion-topic"

    # Ingest click events from Kafka
    clicks_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", click_topic)
        .load()
    )

    clicks = (
        clicks_raw.selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), clickSchema).alias("data"))
        .select("data.*")
        .withWatermark("timestamp", "10 seconds")
    )

    # Ingest conversion events from Kafka
    conversions_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", conversion_topic)
        .load()
    )

    conversions = (
        conversions_raw.selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), conversionSchema).alias("data"))
        .select("data.*")
        .withWatermark("timestamp", "10 seconds")
    )

    # Deduplicate events based on eventId
    deduped_clicks = clicks.dropDuplicates(["eventId"])
    deduped_conversions = conversions.dropDuplicates(["eventId"])

    # Alias DataFrames for clarity in join conditions
    clicks_alias = deduped_clicks.alias("clicks")
    conversions_alias = deduped_conversions.alias("conversions")

    # Join click and conversion events on userId within a 30-minute window
    attributed = clicks_alias.join(
        conversions_alias,
        expr(
            """
            clicks.userId = conversions.userId AND
            conversions.timestamp BETWEEN clicks.timestamp AND clicks.timestamp + interval 30 minutes
        """
        ),
    )

    # Aggregate campaign performance metrics using a 5-minute window
    campaignMetrics = attributed.groupBy(
        col("campaignId"), window(col("clicks.timestamp"), "5 minutes")
    ).count()

    # Write the aggregated metrics to the console sink (for demo purposes)
    query = (
        campaignMetrics.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()
