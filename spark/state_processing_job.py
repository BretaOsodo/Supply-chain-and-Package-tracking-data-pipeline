import json 
import logging 
from datetime import datetime, timezone
import os

import redis 
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql.streaming import StreamingQueryListener
from typing import Dict, Optional, Iterator

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

STATE_ORDER=["picked", "shipped", "in_transit", "out_for_delivery", "delivered"]

STATE_INDEX={s: i for i, s in enumerate(STATE_ORDER)}

def write_to_redis_partition(
    iterator,
    redis_host,
    redis_port,
    redis_db,
    redis_password
):
    try:
        import redis

        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True
        )

        redis_client.ping()

    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return

    pipeline = redis_client.pipeline()
    batch_count = 0

    try:
        for row in iterator:

            state = row.asDict(recursive=True)

            package_id = state.get("package_id")

            if not package_id:
                continue

            state_json = json.dumps(state, default=str)

            pipeline.setex(
                f"package:{package_id}:state",
                30 * 24 * 60 * 60,
                state_json
            )

            pipeline.hset(
                f"package:{package_id}",
                mapping={
                    "current_state": state.get("current_state") or "",
                    "current_location": state.get("current_location") or "",
                    "last_scan_time": state.get("last_scan_time") or "",
                    "total_scans": str(state.get("total_scans", 0)),
                    "completed": str(
                        state.get("completed", False)
                    ).lower()
                }
            )

            pipeline.sadd(
                "packages:active",
                package_id
            )

            if state.get("completed", False):
                pipeline.sadd(
                    "packages:completed",
                    package_id
                )

            batch_count += 1

            if batch_count >= 100:
                pipeline.execute()
                pipeline = redis_client.pipeline()
                batch_count = 0

        if batch_count:
            pipeline.execute()

    except Exception as e:
        logger.error(f"Redis partition write failed: {e}")

    finally:
        redis_client.close()

def write_to_dynamodb_partition(
    iterator,
    table_name,
    region_name,
    endpoint_url=None
):
    """
    Write package state records to DynamoDB from a Spark executor partition.

    This function MUST remain outside PackageStateProcessor so that
    Spark does not try to serialize the SparkSession/SparkContext.
    """

    try:
        import boto3

        dynamodb = boto3.resource(
            "dynamodb",
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )

        table = dynamodb.Table(table_name)

        # Test connection
        table.load()

        logger.info(
            f"DynamoDB connection established: {table_name}"
        )

    except Exception as e:
        logger.error(
            f"Failed to connect to DynamoDB: {e}"
        )
        return

    batch_items = []
    batch_size = 25

    try:

        for row in iterator:

            try:
                state = row.asDict(recursive=True)

                package_id = state.get("package_id")

                if not package_id:
                    continue

                item = {
                    "package_id": package_id,
                    "current_state": state.get(
                        "current_state", ""
                    ),
                    "current_location": state.get(
                        "current_location", ""
                    ),
                    "last_scan_time": state.get(
                        "last_scan_time", ""
                    ),
                    "first_scan_time": state.get(
                        "first_scan_time", ""
                    ),
                    "total_scans": int(
                        state.get("total_scans", 0)
                    ),
                    "completed": bool(
                        state.get("completed", False)
                    ),
                    "state_history": state.get(
                        "state_history", []
                    ),
                    "last_device_id": state.get(
                        "last_device_id", ""
                    ),
                    "processing_timestamp": state.get(
                        "processing_timestamp", ""
                    )
                }

                completion_time = state.get(
                    "completion_time"
                )

                if completion_time:
                    item["completion_time"] = completion_time

                batch_items.append(item)

                # DynamoDB BatchWriteItem limit = 25 items
                if len(batch_items) >= batch_size:

                    with table.batch_writer(
                        overwrite_by_pkeys=["package_id"]
                    ) as batch:

                        for item in batch_items:
                            batch.put_item(Item=item)

                    logger.info(
                        f"Wrote {len(batch_items)} items "
                        f"to DynamoDB"
                    )

                    batch_items = []

            except Exception as e:

                logger.error(
                    f"Failed to prepare DynamoDB item: {e}"
                )

        # Write remaining items
        if batch_items:

            with table.batch_writer(
                overwrite_by_pkeys=["package_id"]
            ) as batch:

                for item in batch_items:
                    batch.put_item(Item=item)

            logger.info(
                f"Wrote {len(batch_items)} remaining items "
                f"to DynamoDB"
            )

    except Exception as e:

        logger.error(
            f"DynamoDB partition write failed: {e}"
        )
class PackageStateProcessor:
    def __init__(
            self,
            bootstrap_servers:str="kafka1:29092,kafka2:29092",
            input_topic:str="scan_events_validated",
            output_topic:str ="state-processed-data",
            consumer_group:str="state-processor",
            redis_host:str="127.0.0.1",
            redis_port:int=6379,
            redis_db:int=0,
            redis_password: Optional[str] = None,
            dynamodb_table: str = "package-tracking",
            dynamodb_region: str = "eu-north-1",
            dynamodb_endpoint: str="http://dynamodb-local:8000",
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
    
                self._redis_client=redis.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    db=self.redis_db,
                    password=self.redis_password,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True
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

    def _update_package_state(self,events_df):
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
        state_udf=udf(self._update_state_udf,self.state_schema)

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
                col("scan_count")
            )
        ).select("state.*")

        return state_df

    @staticmethod
    def _update_state_udf(
        package_id,
        event_history,
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
            "processing_timestamp": datetime.now(timezone.utc).isoformat()
        }

    

    

    def _batch_write_dynamodb(self, table, items):
        """Batch write items to DynamoDB with retry logic."""
        try:
            with table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
            logger.debug(f"Wrote {len(items)} items to DynamoDB")
        except Exception as e:
            logger.error(f"Failed to batch write to DynamoDB: {e}")
            # Fallback to individual writes
            for item in items:
                try:
                    table.put_item(Item=item)
                except Exception as e2:
                    logger.error(f"Failed to write item {item.get('package_id')}: {e2}")

    def _write_to_kafka_batch(self,state_df,batch_id):
        if state_df.count() == 0:
            return
        
        # Convert state to JSON and write to Kafka
        kafka_df = state_df.select(
            to_json(
                struct(
                    col("package_id"),
                    col("current_state"),
                    col("state_history"),
                    col("first_scan_time"),
                    col("last_scan_time"),
                    col("current_location"),
                    col("last_device_id"),
                    col("total_scans"),
                    col("completed"),
                    col("completion_time"),
                    col("processing_timestamp")
                )
            ).alias("value"),
            col("package_id").alias("key")
        )
        # Write to Kafka
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("topic", self.output_topic) \
            .option("kafka.acks", "all") \
            .option("kafka.compression.type", "snappy") \
            .mode("append") \
            .save()
        
        logger.info(f"Batch {batch_id}: Wrote {state_df.count()} states to Kafka")

    def process_state_with_foreach_batch(self):
        """
        Process state using foreachBatch for better control and external system writes.
        """
        logger.info("Starting State Processing Pipeline with foreachBatch...")
        
        # Read from Kafka
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()
        
        # Parse events
        parsed_stream = self._parse_events(raw_stream)

        redis_host = self.redis_host
        redis_port = self.redis_port
        redis_db = self.redis_db
        redis_password = self.redis_password

        dynamodb_table_name = self.dynamodb_table_name
        dynamodb_region = self.dynamodb_region
        dynamodb_endpoint = self.dynamodb_endpoint
                
        # Process state
        def process_batch(df, batch_id):
            if df.count() == 0:
                return
            
            logger.info(f"Processing batch {batch_id} with {df.count()} events")
            
            # Update state
            state_df = self._update_package_state(df)

            if state_df.count() > 0:

                state_rdd=state_df.rdd

                # Write to Redis using foreachPartition
                state_rdd.foreachPartition(
                    lambda partition: write_to_redis_partition(
                        partition,
                        redis_host,
                        redis_port,
                        redis_db,
                        redis_password
                    )
                )

                # Write to DynamoDB using foreachPartition
                state_rdd.foreachPartition(
                    lambda partition: write_to_dynamodb_partition(
                        partition,
                        dynamodb_table_name,
                        dynamodb_region,
                        dynamodb_endpoint
                    )
                )

                # Write to Kafka
                self._write_to_kafka_batch(state_df, batch_id)

                completed = state_df.filter(col("completed") == True).count()
                in_progress = state_df.count() - completed
                logger.info(f"Batch {batch_id}: {completed} completed, {in_progress} in-progress packages")

        query = parsed_stream.writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", f"{self.checkpoint_location}/foreach_batch") \
            .trigger(processingTime="10 seconds") \
            .start()

        return query
    
    def process_state_with_map_groups(self):
        """
        Process state using mapGroupsWithState for more efficient state management.
        This is a more advanced approach with better state handling.
        """
        from pyspark.sql.streaming import GroupState, GroupStateTimeout
        
        logger.info("Starting State Processing Pipeline with mapGroupsWithState...")
        
        # Define state update function
        def update_package_state(package_id: str, events_iter, state: GroupState)-> Iterator[dict]:
            """
            Update package state for each package_id group.
            This function is called for each group of events for a package.
            """
            # Initialize state 
            if state.exists:
                package_state=state.get()

                if isinstance(package_state,Row):
                    package_state=package_state.asDict()
                else:
                    package_state=dict(package_state)
            else:
                package_state = {
                    "package_id": package_id,
                    "current_state": None,
                    "state_history": [],
                    "first_scan_time": None,
                    "last_scan_time": None,
                    "current_location": None,
                    "last_device_id": None,
                    "total_scans": 0,
                    "completed": False,
                    "completion_time": None,
                    "processing_timestamp": None
                }
            
            
            # Process events
            events_list = list(events_iter)
            
            if events_list:
                # Sort by event_time
                sorted_events = sorted(events_list, key=lambda x: x["event_time"] if x["event_time"] is not None else "")
                
                for event in sorted_events:
                    # Update package state
                    package_state["total_scans"] += 1
                    package_state["last_scan_time"] = event["event_time"]
                    package_state["current_state"] = event["scan_type"]
                    package_state["current_location"] = event["location_id"]
                    package_state["last_device_id"] = event["device_id"]
                    package_state["state_history"].append(event["scan_type"])
                    
                    if not package_state["first_scan_time"]:
                        package_state["first_scan_time"] = event["event_time"]
                    
                    if event["scan_type"] == "delivered":
                        package_state["completed"] = True
                        package_state["completion_time"] = event["event_time"]
                
            # Update state
            package_state["processing_timestamp"] = str(datetime.now())
            state.update(package_state)
            
            # Timeout after 1 hour of inactivity
            state.setTimeoutDuration("1 hour")
            
            # Return the updated state
            yield package_state
        
        # Read from Kafka
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse events
        parsed_stream = self._parse_events(raw_stream)


        #make sure the columns we need are present and non-null for grouping 
        events_for_state=(
        parsed_stream
        .filter(col("package_id").isNotNull())
        .select(
            "package_id",
            "scan_type",
            "location_id",
            "device_id",
            "event_time",
            
        )
    )
        
        # Apply state processing using mapGroupsWithState
        state_stream = parsed_stream \
            .groupByKey(lambda row: row["package_id"]) \
            .flatMapGroupsWithState(
                func=update_package_state,
                outputMode="update",
                timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
            )
        
        # Convert state to DataFrame
        state_df = self.spark.createDataFrame(
            state_stream.rdd,
            schema=self.state_schema
        )
        
        # Write to Kafka
        kafka_query = state_df.select(
            to_json(
                struct(
                    col("package_id"),
                    col("current_state"),
                    col("state_history"),
                    col("first_scan_time"),
                    col("last_scan_time"),
                    col("current_location"),
                    col("last_device_id"),
                    col("total_scans"),
                    col("completed"),
                    col("completion_time"),
                    col("processing_timestamp")
                )
            ).alias("value"),
            col("package_id").alias("key")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("topic", self.output_topic) \
            .option("checkpointLocation", f"{self.checkpoint_location}/kafka_output") \
            .outputMode("update") \
            .trigger(processingTime="5 seconds") \
            .start()
        
        # Write to Redis and DynamoDB using foreachBatch
        def write_to_external_systems(df, batch_id):
            if df.count() > 0:
                state_rdd = df.rdd
                state_rdd.foreachPartition(
                    lambda partition: write_to_redis_partition(
                        partition,
                        self.redis_host,
                        self.redis_port,
                        self.redis_db,
                        self.redis_password
                    )
                )
                state_rdd.foreachPartition(
                    lambda partition: write_to_dynamodb_partition(
                        partition,
                        self.dynamodb_table_name,
                        self.dynamodb_region
                    )
                )

        external_writes_query = state_df.writeStream \
            .foreachBatch(write_to_external_systems) \
            .outputMode("update") \
            .trigger(processingTime="5 seconds") \
            .start()

        return kafka_query, external_writes_query
    def process_with_foreach_batch_and_external_writes(self):
        """
        Combine streaming with batch processing for external system writes.
        """

        logger.info(
            "Starting State Processing Pipeline with external writes..."
        )

        # ---------------------------------------------------------
        # Read from Kafka
        # ---------------------------------------------------------

        raw_stream = (
            self.spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                self.bootstrap_servers
            )
            .option(
                "subscribe",
                self.input_topic
            )
            .option(
                "startingOffsets",
                "latest"
            )
            .option(
                "failOnDataLoss",
                "false"
            )
            .load()
        )

        # ---------------------------------------------------------
        # Parse events
        # ---------------------------------------------------------

        parsed_stream = self._parse_events(raw_stream)

        # ---------------------------------------------------------
        # IMPORTANT:
        # Extract configuration BEFORE foreachBatch.
        #
        # These are plain Python values.
        # We DO NOT use self inside foreachPartition.
        # ---------------------------------------------------------

        redis_host = self.redis_host
        redis_port = self.redis_port
        redis_db = self.redis_db
        redis_password = self.redis_password

        dynamodb_table_name = self.dynamodb_table_name
        dynamodb_region = self.dynamodb_region
        dynamodb_endpoint = self.dynamodb_endpoint

        # ---------------------------------------------------------
        # Process each micro-batch
        # ---------------------------------------------------------

        def process_batch(df, batch_id):

            # Check whether batch contains data
            batch_count = df.count()

            if batch_count == 0:
                return

            logger.info(
                f"Processing batch {batch_id} "
                f"with {batch_count} events"
            )

            # -----------------------------------------------------
            # Update package state
            # -----------------------------------------------------

            state_df = self._update_package_state(df)

            state_count = state_df.count()

            if state_count == 0:
                return

            # -----------------------------------------------------
            # Kafka
            #
            # This is running on the DRIVER inside foreachBatch,
            # so using self here is okay.
            # -----------------------------------------------------

            self._write_to_kafka_batch(
                state_df,
                batch_id
            )

            # -----------------------------------------------------
            # Redis
            #
            # DO NOT USE self inside foreachPartition.
            # -----------------------------------------------------

            state_rdd = state_df.rdd

            state_rdd.foreachPartition(
                lambda partition: write_to_redis_partition(
                    partition,
                    redis_host,
                    redis_port,
                    redis_db,
                    redis_password
                )
            )

            # -----------------------------------------------------
            # DynamoDB
            #
            # Again, only plain variables are passed.
            # -----------------------------------------------------

            state_rdd.foreachPartition(
                lambda partition: write_to_dynamodb_partition(
                    partition,
                    dynamodb_table_name,
                    dynamodb_region,
                    dynamodb_endpoint
                )
            )

            # -----------------------------------------------------
            # Logging
            # -----------------------------------------------------

            completed = (
                state_df
                .filter(col("completed") == True)
                .count()
            )

            in_progress = state_count - completed

            logger.info(
                f"Batch {batch_id}: "
                f"{completed} completed, "
                f"{in_progress} in-progress packages"
            )

        # ---------------------------------------------------------
        # Start streaming query
        # ---------------------------------------------------------

        query = (
            parsed_stream.writeStream
            .foreachBatch(process_batch)
            .option(
                "checkpointLocation",
                f"{self.checkpoint_location}/state_processing"
            )
            .trigger(processingTime="10 seconds")
            .start()
        )

        return query
    
    def run_pipeline(self):
        """
        Run the complete state processing pipeline.
        """
        logger.info("=" * 80)
        logger.info("Starting Package Tracking State Processing Pipeline")
        logger.info(f"Input topic: {self.input_topic}")
        logger.info(f"Output topic: {self.output_topic}")
        logger.info(f"Redis: {self.redis_host}:{self.redis_port}")
        logger.info(f"DynamoDB table: {self.dynamodb_table_name}")
        logger.info("=" * 80)
        
        # Choose processing method
        # Option 1: foreachBatch (recommended for external system writes)
        query = self.process_with_foreach_batch_and_external_writes()
        
        
        return query


class StreamingQueryMonitor(StreamingQueryListener):
    """Monitor streaming queries and log their progress."""
    
    def onQueryStarted(self, event):
        logger.info(f"Query started: {event.name} - {event.id}")
    
    def onQueryProgress(self, event):
        if event.progress.numInputRows > 0:
            logger.info(f"Query {event.name}: processed {event.progress.numInputRows} rows, "
                       f"batch duration: {event.progress.batchDuration}ms")
    
    def onQueryTerminated(self, event):
        if event.exception:
            logger.error(f"Query terminated with error: {event.exception}")
        else:
            logger.info(f"Query terminated: {event.id}")


def main():
    """Main entry point for the state processing pipeline."""
    
    # Configuration
    KAFKA_BROKER = "kafka1:29092,kafka2:29092"
    INPUT_TOPIC = "scan_events_validated"
    OUTPUT_TOPIC = "state-processed-data"
    
    REDIS_HOST = "redis"
    REDIS_PORT = 6379
    REDIS_DB = 0
    
    DYNAMODB_TABLE = "package-tracking-state"
    DYNAMO_ENDPOINT="http://dynamodb-local:8000"
    CHECKPOINT_LOCATION = "/tmp/checkpoints/state_processing"
    
    # Optional: Spark configurations
    spark_config = {
        "spark.sql.streaming.stateStore.providerClass": 
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        "spark.sql.streaming.stateStore.compression.codec": "lz4",
        "spark.sql.streaming.noDataProgressEventInterval": "10s"
    }
    
    # Initialize pipeline
    pipeline = PackageStateProcessor(
        bootstrap_servers=KAFKA_BROKER,
        input_topic=INPUT_TOPIC,
        output_topic=OUTPUT_TOPIC,

        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        redis_db=REDIS_DB,
        
        dynamodb_table=DYNAMODB_TABLE,
        dynamodb_region="eu-north-1",
        dynamodb_endpoint=DYNAMO_ENDPOINT,

        checkpoint_location=CHECKPOINT_LOCATION,
        spark_config=spark_config
    )
    
    # Add streaming listener
    monitor = StreamingQueryMonitor()
    pipeline.spark.streams.addListener(monitor)
    
    try:
        # Run pipeline
        query = pipeline.run_pipeline()
        
        logger.info("Pipeline started. Waiting for termination...")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise


if __name__ == "__main__":
    from datetime import datetime
    main()