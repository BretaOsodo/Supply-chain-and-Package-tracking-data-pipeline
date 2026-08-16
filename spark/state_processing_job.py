import json 
import logging 
import os 
from datetime import datetime, timezone

import redis 
import boto3
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.state import GroupStateTimeout

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)


#configurations 
class config:
    KAFKA_BROKER="kafka1:29092,kafka2:29092"
    INPUT_TOPIC="scan_events_validated"
    OUTPUT_TOPIC="package_state_changes"

    REDIS_HOST=os.getenv("REDIS_HOST","redis")
    REDIS_PORT= int(os.getenv("REDIS_PORT",6379))
    REDIS_DB=int(os.getenv("REDIS_DB",0))
    REDIS_PASSWORD=os.getenv("REDIS_PASSWORD")
    REDIS_TTL=int(os.getenv("REDIS-TTL", 7*86400))

    DYNAMODB_TABLE="package_tracking_state"
    DYNAMODB_REGION="eu-north-1"
    DYNAMODB_ENDPOINT=os.getenv("DYNAMODB_ENDPOINT")

    AWS_ACCESS_KEY=os.getenv("AWS_ACCES_KEY")
    AWS_SECRET_KEY=os.getenv("AWS_SECRET_KEY")

    CHECKPOINT_LOCATION ="./checkpoints/state-processor"
    PROCESSING_TIME="5 seconds"
    MAX_OFFSETS_PER_TRIGGER=1000
    WATERMARK_DELAY="2 hours"
    STAET_TIMEOUT_HOURS=48

    @classmethod
    def get_spark_config(cls)-> dict:
        return {
            "spark.sql.streaming.stateStore.providerClass":
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
            "spark.sql.streaming.stateStore.rocksdb.blockSizeKB": "4",
            "spark.sql.streaming.stateStore.rocksdb.maxOpenFiles": "-1",
            "spark.sql.shuffle.partitions": "8",
        }

