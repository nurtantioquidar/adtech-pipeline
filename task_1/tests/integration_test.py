import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class ConversionAttributionIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("ConversionAttributionIntegrationTest")
            .getOrCreate()
        )
        # For testing purposes, reduce the number of shuffle partitions
        cls.spark.conf.set("spark.sql.shuffle.partitions", "1")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_conversion_attribution_pipeline(self):
        # Define schemas for clicks and conversions
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

        # Create static test data
        click_data = [
            (
                "user1",
                "campaign1",
                datetime.strptime("2025-03-01T12:00:00", "%Y-%m-%dT%H:%M:%S"),
                "click1",
            ),
            (
                "user2",
                "campaign2",
                datetime.strptime("2025-03-01T12:05:00", "%Y-%m-%dT%H:%M:%S"),
                "click2",
            ),
        ]
        conversion_data = [
            (
                "user1",
                datetime.strptime("2025-03-01T12:15:00", "%Y-%m-%dT%H:%M:%S"),
                "conv1",
            ),
            (
                "user2",
                datetime.strptime("2025-03-01T12:40:00", "%Y-%m-%dT%H:%M:%S"),
                "conv2",
            ),  # outside a 30-min window for user2
        ]

        # Create DataFrames for clicks and conversions
        clicks_df = self.spark.createDataFrame(click_data, schema=clickSchema)
        conversions_df = self.spark.createDataFrame(
            conversion_data, schema=conversionSchema
        )

        # Apply watermarking to simulate late arrivals handling
        clicks_df = clicks_df.withWatermark("timestamp", "10 seconds")
        conversions_df = conversions_df.withWatermark("timestamp", "10 seconds")

        # Deduplicate events using dropDuplicates on eventId
        deduped_clicks = clicks_df.dropDuplicates(["eventId"])
        deduped_conversions = conversions_df.dropDuplicates(["eventId"])

        # For integration testing, we simulate the join operation (attribution)
        # Note: Since column names are identical in both DataFrames, aliasing is required.
        clicks_alias = deduped_clicks.alias("clicks")
        conversions_alias = deduped_conversions.alias("conversions")

        # Join clicks and conversions on userId with a 30-minute attribution window
        attributed = clicks_alias.join(
            conversions_alias,
            expr(
                """
                clicks.userId = conversions.userId AND
                conversions.timestamp BETWEEN clicks.timestamp AND clicks.timestamp + interval 30 minutes
            """
            ),
        )

        # Write the output to an in-memory table using Trigger.Once mode for testing
        query = (
            attributed.writeStream.format("memory")
            .queryName("attribution_results")
            .outputMode("append")
            .trigger(once=True)
            .start()
        )

        # Wait until the streaming query finishes processing
        query.awaitTermination()

        # Read the output from the in-memory table
        result_df = self.spark.sql("SELECT * FROM attribution_results")
        results = result_df.collect()

        # Validate the results: expecting one valid attribution (user1 only)
        self.assertEqual(len(results), 1)

        # Additional assertions can validate that the attribution contains the expected campaign ID and timestamps.
        attributed_row = results[0]
        self.assertEqual(attributed_row["campaignId"], "campaign1")
        self.assertEqual(attributed_row["userId"], "user1")
        # You can also assert timestamp values if necessary.


if __name__ == "__main__":
    unittest.main()
