
"""


Reads state-processed package events from Kafka, checks actual scan times
against ETA data in Redis, and publishes delay notifications to Kafka.

Architecture:
  Input:  Kafka topic 'state-processed-data'
  Lookup: Redis (eta:{package_id} hash, written by ETA Calculator)
  Output: Kafka topic 'delay-notifications'
  DLQ:    Kafka topic 'delay-detector-dlq'
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional, Iterator, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    ArrayType, IntegerType, BooleanType, LongType
)
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp, lit
)

import redis

# Configuration

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka1:29092,kafka2:29092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "state-processed")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "delay-notifications")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "delay-detector-dlq")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/spark/checkpoints/delay_detector")

DELAY_THRESHOLD_MINUTES = int(os.getenv("DELAY_THRESHOLD_MINUTES", "15"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)


# Logging (driver-side only)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Schemas


# Input: state-processed events (exact schema you provided)
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

# Output: delay notifications
NOTIFICATION_SCHEMA = StructType([
    StructField("package_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("scan_type", StringType(), False),
    StructField("actual_timestamp", TimestampType(), False),
    StructField("expected_timestamp", TimestampType(), False),
    StructField("delay_minutes", LongType(), False),
    StructField("location", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("message", StringType(), False),
    StructField("detected_at", TimestampType(), False)
])

# Executor-Side Helpers (module-level for Spark serialization)


def parse_iso8601(ts_str: str) -> Optional[datetime]:
    """Parse ISO 8601 timestamp to Python datetime."""
    if not ts_str:
        return None
    try:
        # Python < 3.11 does not accept 'Z'; replace with explicit UTC offset
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        return datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        return None


def process_partition(
    partition_iter: Iterator,
    redis_cfg: Dict[str, Any],
    delay_threshold: int
) -> Iterator[Dict[str, Any]]:
    """
    Executor-side delay detection with batched Redis Pipeline lookups.

    Why mapPartitions?
      - One Redis connection per partition (not per row).
      - Pipeline batches all HGETALL commands into a single network round-trip.
      - Keeps all processing distributed; no data is collected to the driver.
    """
    # 1. Materialize partition rows into plain dicts
    rows = []
    for row in partition_iter:
        try:
            rows.append(row.asDict())
        except Exception:
            continue

    if not rows:
        return iter([])

    # 2. Open Redis connection for this partition
    try:
        client = redis.Redis(
            host=redis_cfg["host"],
            port=redis_cfg["port"],
            db=redis_cfg["db"],
            password=redis_cfg["password"],
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30
        )
        client.ping()
    except Exception:
        # Redis unreachable: skip this entire partition gracefully
        return iter([])

    # 3. Build aligned list of rows that have a package_id
    valid_rows = []
    package_ids = []
    for r in rows:
        pid = r.get("package_id")
        if pid:
            valid_rows.append(r)
            package_ids.append(pid)

    if not package_ids:
        client.close()
        return iter([])

    # 4. Batch lookup via Redis Pipeline (single round-trip)
    pipe = client.pipeline()
    for pid in package_ids:
        pipe.hgetall(f"eta:{pid}")

    try:
        results = pipe.execute()
    except Exception:
        client.close()
        return iter([])

    # 5. Detect delays
    notifications = []
    for row, eta_data in zip(valid_rows, results):
        try:
            if not eta_data or not isinstance(eta_data, dict):
                continue

            expected_str = eta_data.get("expected_timestamp")
            actual_str = row.get("last_scan_time")
            if not expected_str or not actual_str:
                continue

            expected_dt = parse_iso8601(expected_str)
            actual_dt = parse_iso8601(actual_str)
            if not expected_dt or not actual_dt:
                continue

            delay_minutes = int((actual_dt - expected_dt).total_seconds() // 60)
            if delay_minutes <= delay_threshold:
                continue

            # Severity tiers
            if delay_minutes > 240:
                severity = "CRITICAL"
                msg = f"Package is critically delayed by {delay_minutes} minutes (>4 hours)"
            elif delay_minutes > 120:
                severity = "HIGH"
                msg = f"Package is significantly delayed by {delay_minutes} minutes (>2 hours)"
            elif delay_minutes > 60:
                severity = "MEDIUM"
                msg = f"Package is moderately delayed by {delay_minutes} minutes (>1 hour)"
            else:
                severity = "LOW"
                msg = f"Package is slightly delayed by {delay_minutes} minutes"

            notifications.append({
                "package_id": row["package_id"],
                "event_type": "DELAY_DETECTED",
                "scan_type": row.get("current_state", "UNKNOWN"),
                "actual_timestamp": actual_dt,
                "expected_timestamp": expected_dt,
                "delay_minutes": delay_minutes,
                "location": row.get("current_location", "UNKNOWN"),
                "severity": severity,
                "message": msg,
                "detected_at": datetime.now(timezone.utc)
            })

        except Exception:
            # Unrecoverable per-record error: skip silently (do not crash partition)
            continue

    client.close()
    return iter(notifications)



# Driver-Side Job Class


class DelayDetectorJob:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.redis_config = {
            "host": REDIS_HOST,
            "port": REDIS_PORT,
            "db": REDIS_DB,
            "password": REDIS_PASSWORD
        }

    def _create_spark_session(self) -> SparkSession:
        spark = SparkSession.builder \
            .appName("PackageDelayDetector") \
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
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "50000") \
            .load()

    def _process_batch(self, df, batch_id: int):
        """
        foreachBatch logic:
          1. Parse JSON (PERMISSIVE).
          2. Route corrupt/missing records to DLQ.
          3. Detect delays via mapPartitions + Redis Pipeline.
          4. Write notifications and DLQ to Kafka.
        """
        # 
        # Parse raw Kafka values
        # 
        parsed = df.select(
            from_json(
                col("value").cast("string"),
                INPUT_SCHEMA,
                {"mode": "PERMISSIVE"}
            ).alias("data"),
            col("value").cast("string").alias("raw_value")
        )

        # 
        # DLQ: corrupt JSON or missing required fields
        # 
        dlq_df = parsed.filter(
            (col("data._corrupt_record").isNotNull()) |
            (col("data.package_id").isNull()) |
            (col("data.last_scan_time").isNull())
        ).select(
            to_json(struct(
                col("raw_value").alias("original_record"),
                lit("PARSE_ERROR").alias("error_type"),
                lit("Corrupt JSON or missing package_id/last_scan_time").alias("error_message"),
                current_timestamp().alias("timestamp")
            )).alias("value"),
            lit(None).cast("string").alias("key")
        )

        
        # Valid records for delay detection
        
        good_df = parsed.filter(
            (col("data._corrupt_record").isNull()) &
            (col("data.package_id").isNotNull()) &
            (col("data.last_scan_time").isNotNull())
        ).select("data.*")

        # Distributed delay detection (no data collected to driver)
        
        redis_cfg = self.redis_config
        threshold = DELAY_THRESHOLD_MINUTES

        notification_rdd = good_df.rdd.mapPartitions(
            lambda part: process_partition(part, redis_cfg, threshold)
        )

        notification_df = self.spark.createDataFrame(
            notification_rdd,
            schema=NOTIFICATION_SCHEMA
        )

    
        # Write notifications to Kafka

        notification_df.select(
            to_json(struct(
                col("package_id"),
                col("event_type"),
                col("scan_type"),
                col("actual_timestamp"),
                col("expected_timestamp"),
                col("delay_minutes"),
                col("location"),
                col("severity"),
                col("message"),
                col("detected_at")
            )).alias("value"),
            col("package_id").alias("key")
        ).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("topic", OUTPUT_TOPIC) \
            .option("kafka.acks", "all") \
            .option("kafka.compression.type", "snappy") \
            .mode("append") \
            .save()

        # Write DLQ to Kafka
        dlq_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
            .option("topic", DLQ_TOPIC) \
            .option("kafka.acks", "all") \
            .mode("append") \
            .save()

        
        # Metrics logging (optional; triggers small post-write jobs)
        
        notif_count = notification_df.count()
        dlq_count = dlq_df.count()
        if notif_count > 0:
            logger.info(f"Batch {batch_id}: {notif_count} delay notifications sent")
        if dlq_count > 0:
            logger.warning(f"Batch {batch_id}: {dlq_count} records sent to DLQ")

    def run(self):
        logger.info("=" * 70)
        logger.info("Starting Package Delay Detector")
        logger.info(f"Input:  {INPUT_TOPIC}")
        logger.info(f"Output: {OUTPUT_TOPIC}")
        logger.info(f"DLQ:    {DLQ_TOPIC}")
        logger.info(f"Redis:  {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
        logger.info(f"Threshold: {DELAY_THRESHOLD_MINUTES} minutes")
        logger.info(f"Checkpoint: {CHECKPOINT_LOCATION}")
        logger.info("=" * 70)

        raw_stream = self._read_kafka()

        query = raw_stream.writeStream \
            .foreachBatch(self._process_batch) \
            .option("checkpointLocation", CHECKPOINT_LOCATION) \
            .trigger(processingTime=TRIGGER_INTERVAL) \
            .start()

        query.awaitTermination()



# Entry Point


def main():
    job = DelayDetectorJob()
    job.run()


if __name__ == "__main__":
    main()