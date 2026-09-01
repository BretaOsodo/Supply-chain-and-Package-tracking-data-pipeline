
"""
Analytics Aggregator — PySpark Structured Streaming Job

Reads state-processed package events from Kafka, computes real-time analytics,
and writes aggregated metrics to PostgreSQL.

Architecture:
  Input:  Kafka topic 'state-processed-data'
  Output: PostgreSQL table 'package_analytics'
  Scope:  Business intelligence & dashboard metrics only.
          Does NOT do delay detection, notifications, or Redis lookups.
"""

import os
import logging
from datetime import datetime
from pyspark.sql import functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    ArrayType, IntegerType, BooleanType, LongType, DoubleType
)
from pyspark.sql.functions import (
    col, from_json, window, count, avg, sum as spark_sum,
    countDistinct, current_timestamp, lit, when, max as spark_max, min as spark_min
)

# 
# Configuration
# 

KAFKA_BROKERS = "kafka1:29092,kafka2:29092"
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "state-processed-data")
CHECKPOINT_LOCATION = "/spark/checkpoints/analytics"

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("HOST_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "supply_chain")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "package_analytics")

# Window Configuration
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "5 minutes")   # Tumbling window
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "10 minutes")  # Late data tolerance
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

# 
# Logging
# 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 
# Schemas
# 

INPUT_SCHEMA = StructType([
    StructField("package_id", StringType(), True),
    StructField("current_state", StringType(), True),
    StructField("state_history", ArrayType(StringType()), True),
    StructField("first_scan_time", StringType(), True),
    StructField("last_scan_time", StringType(), True),
    StructField("current_location", StringType(), True),
    StructField("last_device_id", StringType(), True),
    StructField("total_scans", IntegerType(), True),
    StructField("completed", BooleanType(), True),
    StructField("processing_timestamp", StringType(), True)
])

# 
# PostgreSQL JDBC Helper
# 

def get_jdbc_url() -> str:
    """Build PostgreSQL JDBC URL."""
    return f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def get_jdbc_properties() -> dict:
    """Build JDBC connection properties."""
    return {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
        "isolationLevel": "READ_UNCOMMITTED"
    }

def create_analytics_table_if_not_exists():
    """
    Ensure the target PostgreSQL table exists.
    Run this once on the driver before starting the stream.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor

    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {POSTGRES_TABLE} (
                window_start TIMESTAMP NOT NULL,
                window_end TIMESTAMP NOT NULL,
                location VARCHAR(64) NOT NULL,
                total_events BIGINT NOT NULL DEFAULT 0,
                unique_packages BIGINT NOT NULL DEFAULT 0,
                avg_scans_per_package DOUBLE PRECISION NOT NULL DEFAULT 0,
                completed_packages BIGINT NOT NULL DEFAULT 0,
                in_transit_packages BIGINT NOT NULL DEFAULT 0,
                picked_packages BIGINT NOT NULL DEFAULT 0,
                out_for_delivery_packages BIGINT NOT NULL DEFAULT 0,
                delayed_packages BIGINT NOT NULL DEFAULT 0,
                max_scans BIGINT NOT NULL DEFAULT 0,
                min_scans BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (window_start, window_end, location)
            )
        """)
        # Index for dashboard queries
        cursor.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{POSTGRES_TABLE}_time 
            ON {POSTGRES_TABLE}(window_end DESC)
        """)
        conn.commit()
        logger.info(f"PostgreSQL table '{POSTGRES_TABLE}' is ready")
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL table: {e}")
        raise
    finally:
        if conn:
            conn.close()


# Spark Job


class AnalyticsAggregatorJob:
    def __init__(self):
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        spark = SparkSession.builder \
            .appName("PackageAnalyticsAggregator") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _read_kafka(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("subscribe", INPUT_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "50000") \
            .load()

    def _process_batch(self, df, batch_id: int):
        """
        foreachBatch logic:
          1. Parse events
          2. Aggregate by tumbling window + location
          3. Upsert results to PostgreSQL
        """
        if df.isEmpty():
            return

        # Parse JSON
        parsed = df.select(
            from_json(
                col("value").cast("string"),
                INPUT_SCHEMA,
                {"mode": "PERMISSIVE"}
            ).alias("data")
        ).select("data.*").filter(
            col("package_id").isNotNull() & col("current_location").isNotNull()
        )

        if parsed.isEmpty():
            return

        # ---------------------------------------------------------------------
        # Streaming Aggregation with Event Time Windows
        # ---------------------------------------------------------------------
        # We use processing_timestamp as the event time for windowing.
        # If you prefer last_scan_time, change the timestamp column below.
        # ---------------------------------------------------------------------
        aggregated = parsed \
            .withColumn("event_time", col("processing_timestamp").cast(TimestampType())) \
            .withWatermark("event_time", WATERMARK_DELAY) \
            .groupBy(
                window(col("event_time"), WINDOW_DURATION).alias("time_window"),
                col("current_location").alias("location")
            ) \
            .agg(
                F.count("*").alias("total_events"),
                countDistinct("package_id").alias("unique_packages"),
                avg("total_scans").alias("avg_scans_per_package"),
                spark_max("total_scans").alias("max_scans"),
                spark_min("total_scans").alias("min_scans"),
                spark_sum(when(col("completed") == True, 1).otherwise(0)).alias("completed_packages"),
                spark_sum(when(col("current_state") == "in_transit", 1).otherwise(0)).alias("in_transit_packages"),
                spark_sum(when(col("current_state") == "picked", 1).otherwise(0)).alias("picked_packages"),
                spark_sum(when(col("current_state") == "out_for_delivery", 1).otherwise(0)).alias("out_for_delivery_packages"),
                spark_sum(when(col("current_state") == "delayed", 1).otherwise(0)).alias("delayed_packages")
            ) \
            .select(
                col("time_window.start").alias("window_start"),
                col("time_window.end").alias("window_end"),
                col("location"),
                col("total_events"),
                col("unique_packages"),
                col("avg_scans_per_package"),
                col("completed_packages"),
                col("in_transit_packages"),
                col("picked_packages"),
                col("out_for_delivery_packages"),
                col("delayed_packages"),
                col("max_scans"),
                col("min_scans"),
                current_timestamp().alias("updated_at")
            )

        # ---------------------------------------------------------------------
        # Write to PostgreSQL via JDBC
        # ---------------------------------------------------------------------
        # Mode: append (each microbatch writes new window aggregations)
        # In production, you may want to use a staging table + UPSERT
        # or overwrite specific partitions. Here we use simple append
        # with ON CONFLICT handling done at the DB level if needed.
        # ---------------------------------------------------------------------
        try:
            aggregated.write \
                .format("jdbc") \
                .option("url", get_jdbc_url()) \
                .option("dbtable", POSTGRES_TABLE) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .option("batchsize", "1000") \
                .mode("append") \
                .save()

            count = aggregated.count()
            logger.info(f"Batch {batch_id}: Wrote {count} aggregated rows to PostgreSQL")

        except Exception as e:
            logger.error(f"Batch {batch_id}: Failed to write to PostgreSQL: {e}")
            raise

    def run(self):
        logger.info("=" * 70)
        logger.info("Starting Package Analytics Aggregator")
        logger.info(f"Input:     {INPUT_TOPIC}")
        logger.info(f"Output:    {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}.{POSTGRES_TABLE}")
        logger.info(f"Window:    {WINDOW_DURATION}")
        logger.info(f"Watermark: {WATERMARK_DELAY}")
        logger.info("=" * 70)

        # Ensure target table exists
        create_analytics_table_if_not_exists()

        raw_stream = self._read_kafka()

        query = raw_stream.writeStream \
            .foreachBatch(self._process_batch) \
            .option("checkpointLocation", CHECKPOINT_LOCATION) \
            .trigger(processingTime=TRIGGER_INTERVAL) \
            .start()

        query.awaitTermination()



# Entry Point


def main():
    job = AnalyticsAggregatorJob()
    job.run()


if __name__ == "__main__":
    main()