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
from pyspark.sql.streaming.state import GroupStateTimeout,GroupState
from typing import Dict, Optional, Tuple, Iterator

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

STATE_ORDER=["picked", "shipped", "in_transit", "out_for_delivery", "delivered"]

STATE_INDEX={s: i for i, s in enumerate(STATE_ORDER)}
class PackageStateProcessor:
    def __init__(
            self,
            bootstrap_servers:str="kafka1:29092,kafka2:29092",
            input_topic:str="scan_events_validated",
            output_topic:str ="state-processed-data",
            consumer_group:str="state-processor",
            redis_host:str="localhost",
            redis_port:int=6379,
            redis_db:int=0,
            redis_password: Optional[str] = None,
            dynamodb_table: str = "package-tracking",
            dynamodb_region: str = "",
            dynamodb_endpoint: Optional[str] = None,
            checkpoint_location: str = "./checkpoints/state_processor",
            watermark_delay: str = "2 hours",
            state_timeout: str = "7 days",
            spark_config: dict = None):

        self.bootstrap_servers=bootstrap_servers
        self.input_topic = input_topic
        self.output_topic= output_topic
        self.consumer_group=consumer_group 
        self.redis_host=redis_host
        self.redis_port=redis_port
        self.redis_db=redis_db
        self.redis_password=redis_password
        self.dynamodb_table_name = dynamodb_table
        self.dynamodb_region = dynamodb_region
        self.dynamodb_endpoint = dynamodb_endpoint
        self.checkpoint_location = checkpoint_location
        self.watermark_delay = watermark_delay
        self.state_timeout = state_timeout
 
        self.spark = self._create_spark_session(spark_config)
 
        self.input_schema = self._create_input_schema()
        self.state_schema = self._create_state_schema()
 
        self._redis_client = None
        self._dynamodb_client= None

    def _create_spark_session(self,spark_config:dict=None)->SparkSession:
        builder = SparkSession.builder\
                .appName("PackageStateProcessor") \
                .config("spark.sql.shuffle.partitions", "8") \
                .config("spark.sql.streaming.stateStore.providerClass",
                        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
                .config("spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion", "2") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.session.timeZone", "UTC")\
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location)

        if spark_config:
            for key, value in spark_config.items():
                builder=builder.config(key,value)

        spark=builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        return spark

    def _create_input_schema(self)->StructType:
        return StructType([
            StructField("package_id", StringType(), True),
            StructField("scan_type", StringType(), True),
            StructField("location_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("available_time", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("is_malformed", BooleanType(), True),
            StructField("is_valid", BooleanType(), True),
            StructField("validation_errors", StringType(), True),
            StructField("kafka_timestamp", StringType(), True),
            StructField("processing_timestamp", StringType(), True)
        ])

    def _create_state_schema(self)->StructType:
        return StructType([
            StructField("package_id", StringType(), False),
            StructField("current_state", StringType(), False),
            StructField("state_history", ArrayType(StringType()), True),
            StructField("first_scan_time", StringType(), True),
            StructField("last_scan_time", StringType(), True),
            StructField("current_location", StringType(), True),
            StructField("last_device_id", StringType(), True),
            StructField("total_scans", IntegerType(), True),
            StructField("completed", BooleanType(), True),
            StructField("completion_time", StringType(), True),
            StructField("processing_timestamp", StringType(), True)
        ])

    def _get_redis_client(self):
        """Get or create Redis client"""
        if self._redis_client is None:
            try:
                import redis 
                self.redis_client=redis.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    db=self.redis_db,
                    decode_response=True
                )

                #test connection 
                self._redis_client.ping()
                logger.info(f"connected to redis at {self.redis_host}:{self.redis_port}")
            except ImportError:
                logger.warning('Redis librry not installed. Redis functionality disabled')

                self._redis_client=None
            except Exception as e:
                logger.error(f"Failed to connect tp redis:{e}")

                self._redis_client=None
        return self._redis_client
    def _get_dynamodb_client(self):
        """Get or create DynamoDB client"""
        if self._dynamodb_client is None:
            try:
                import boto3
                self._dynamodb_client=boto3.client('dynamodb')

                #Test connection 
                self._dynamodb_client.list_tables()
                logger.info(f"Connected to DynamoDB")

            except ImportError:
                logger.warning("Boto3 library not installed. DynamoDB functional disabled")
                self._dynamodb_client=None

            except Exception as e:
                logger.error(f"Failed to connect to DynamoDB:{e}")
                self._dynamodb_client=None
        return self._dynamodb_client

    def _parse_events(self,validated_df):
        """
        Parse the validated events 
        """

        return validated_df.select(
            from_json(
                col("value").cast("string"),
                self.input_schema,
                {"mode": "PERMISSIVE"}
            ).alias("data"),
            col("offset"),
            col("partition"),
            col("timestamp").alias("kafka_receive_time")
        ).select(
            col("data.*"),
            col("offset"),
            col("partition"),
            col("kafka_receive_time")
        )

    def _updated_package_state(self,events_df):
        """
        Update package state using groupBy and aggregate functions.
        This maintains state across streaming batches.
        
        Args:
            events_df: DataFrame with parsed events
            
        Returns:
            DataFrame with updated state
        """

        #convert event_time to timestamp
        events_with_time=events_df.withColumn(
            "event_timestamp",
            to_timestamp(col("event_time"))
        )

        #group by package_id and update state

        #create a struct with all event details 
        state_udf=udf(self._updated_state_udf,self.state_schema)

        #aggregate by package_id
        state_df=events_with_time.groupBy("package_id").agg(
            collect_list(
                struct(
                    col("scan_type"),
                    col("location_id"),
                    col("device_id"),
                    col("event_timestamp"),
                    col("event_time")
                )
            ).alias("event_history"),
            max("event_timestamp").alias("last_event_time"),
            min("event_timestamp").alias("first_event_time"),
            count("*").alias("scan_count")
        ).withColumn(
            "state",
            state_udf(
                col("package_id"),
                col("event_history"),
                col("last_event_time"),
                col("first_event_time"),
                col("scan_count")
            )
        ).select("state.*")

        return state_df

    @staticmethod
    def _update_state_udf(
        package_id,
        event_history,
        last_event_time,
        first_event_time,
        scan_count
    ):

        if not event_history:
            return None

        #sort events by timestamp
        sorted_events=sorted(event_history,key=lambda x:x["event_timestamp"])

        #determine current state (latest scan_type)
        latest_scan=sorted_events[-1]["scan_type"]

        #build state History
        state_history= [event['scan_type'] for event in sorted_events]

        # Check if delivered
        is_completed = latest_scan == "delivered"
        
        # Get current location
        current_location = sorted_events[-1]["location_id"]
        last_device = sorted_events[-1]["device_id"]
        
        # Format timestamps
        first_scan_time = str(sorted_events[0]["event_time"]) if sorted_events else None
        last_scan_time = str(sorted_events[-1]["event_time"]) if sorted_events else None
        completion_time = str(sorted_events[-1]["event_time"]) if is_completed else None
        
        return {
            "package_id": package_id,
            "current_state": latest_scan,
            "state_history": state_history,
            "first_scan_time": first_scan_time,
            "last_scan_time": last_scan_time,
            "current_location": current_location,
            "last_device_id": last_device,
            "total_scans": scan_count,
            "completed": is_completed,
            "completion_time": completion_time,
            "processing_timestamp": current_timestamp().cast("string")
        }