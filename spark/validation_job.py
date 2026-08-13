from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from typing import Dict
import time 
import logging 
import os 


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS= "127.0.0.1:9092,127.0.0.1:9093"
KAFKA_INPUT_TOPIC = "package_events"
KAFKA_VALIDATED_TOPIC="scan_events_validated"
KAFKA_INVALIDATED_TOPIC="scan_events_dlq"

CHECKPOINT_LOCATION="./checkpoints/validation"
S3_BUCKET_VALIDATED="supply-chain-and-package-tracking-validated"

EVENT_SCHEMA=StructType([
    StructField("package_id",StringType(),True),
    StructField("scan_type",StringType(),True),
    StructField("location_id",StringType(),True),
    StructField("event_time",StringType(),True),
    StructField("available_time",StringType(),True),
    StructField("device_id",StringType(),True),
    StructField('is_malformed',BooleanType(),True),
    StructField("malformed_type",StringType(),True)
])

VALID_SCAN_TYPES=[
    "picked",
    "shipped",
    "in_transit",
    "out_for_delivery",
    "delivered"
]

VALID_DEVICE_PREFIXES=[
    "WH-SCANNER", "HUB-SCANNER", "TRUCK-SCANNER", "DELIVERY-SCANNER"
]


class ValidationPipeline:
    """
    Pyspark streaming pipeline for package tracking data validation
    """

    def __init__(
            self,
            bootstrap_servers:str=KAFKA_BOOTSTRAP_SERVERS,
            input_topic:str=KAFKA_INPUT_TOPIC,
            validated_topic:str=KAFKA_VALIDATED_TOPIC,
            invalidated_topic:str=KAFKA_INVALIDATED_TOPIC,
            s3_bucket=f"s3a://{S3_BUCKET_VALIDATED}",
            checkpoint_location=CHECKPOINT_LOCATION,
            spark_config:dict=None
    ):

        self.bootstrap_servers=bootstrap_servers
        self.input_topic=input_topic
        self.validated_topic = validated_topic
        self.invalidated_topic=invalidated_topic
        self.s3_bucket=s3_bucket
        self.checkpoint_location=checkpoint_location


        #initalize spark session with s3 support 
        self.spark = self._create_spark_session(spark_config)

        #define schema
        self.input_schema = self._create_input_schema()
        self.validation_results_schema= self._create_validation_schema()

        #validation rules
        self.validation_rules=self._create_validation_rules()


    def _create_spark_session(self,spark_config: dict=None)->SparkSession:
        """
        Create spark session with necessary configurations
        """

        builder = SparkSession.builder\
            .appName("PackageTrackingDataValidation") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.streaming.stateStore.providerClass", 
                   "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
            .config("spark.sql.streaming.schemaInference", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.kafka.maxRatePerPartition", "1000") \
            .config("spark.sql.streaming.backpressure.enabled", "true") \
            .config("spark.sql.streaming.backpressure.initialRate", "100")\
            .config(
                    "spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
                )

        #add s3/Hadoop configurations
        builder= builder\
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
            .config("spark.hadoop.fs.s3a.max.total.tasks", "5")

    def _creat_input_schema(self)->StructType:

        return EVENT_SCHEMA

    def _create_validation_schema(self)->StructType:
        """
        Define schema for validation results
        """
        return StructType([
            StructField("is_valid",BooleanType(),True),
            StructField("valdation_error",StringType(),True),
            StructField("validation_timestamp",TimestampType(),False)

        ])

    def _create_validation_rules(self)->Dict:

        return {
            "required_fields":{
              "package_id": lambda x: x is not None and x.startswith("PKG-"),
                "scan_type": lambda x: x is not None and x in [
                    "picked", "shipped", "in_transit", 
                    "out_for_delivery", "delivered"
                ],
                "location_id": lambda x: x is not None and len(x) > 0,
                "event_time": lambda x: x is not None,
                "device_id": lambda x: x is not None and len(x) > 0  
            },
             "format_rules": {
                "package_id": "Must start with 'PKG-' prefix",
                "scan_type": "Must be one of: picked, shipped, in_transit, out_for_delivery, delivered",
                "location_id": "Cannot be null or empty",
                "timestamp": "Must be a valid ISO 8601 timestamp",
                "device_id": "Cannot be null or empty"
            }
        }

    def validate_event(self,event_df):

        #define the validatio functions as UDFs
        def validate_package(package_id):
            if package_id is None:
                return (False,"package_id is null")

            if not package_id.startswith("PKG-"):
                return (False, "package_id missing PKG_ prefix")

            return (True, None)

        def validate_scan_type(scan_type):
            valid_types = ["picked", "shipped", "in_transit", "out_for_delivery", "delivered"]
            if scan_type is None:
                return (False, "scan_type is null")
            if scan_type not in valid_types:
                return (False, f"invalid scan_type: {scan_type}")
            return (True, None)

        def validate_location(location_id):
             if location_id is None:
                return (False, "location_id is null")
             if len(str(location_id).strip()) == 0:
                return (False, "location_id is empty")
             return (True, None)

        def validate_device_id(device_id):
            if device_id is None:
                return (False, "device_id is null")
            if len(str(device_id).strip()) == 0:
                return (False, "device_id is empty")
            return (True, None)

        #create a UDF that returns validation results 
        def validate_event_udf(
                package_id,scan_type,location_id,timestamp,device_id
        ):
            errors=[]

            #Check each field
            if package_id is None or not str(package_id).startswith("PKG-"):
                errors.append("package_id invalid (must start with PKG-)")
            
            valid_types = ["picked", "shipped", "in_transit", "out_for_delivery", "delivered"]
            if scan_type is None or scan_type not in valid_types:
                errors.append(f"scan_type invalid: {scan_type}")
            
            if location_id is None or len(str(location_id).strip()) == 0:
                errors.append("location_id is null or empty")
            
            if timestamp is None:
                errors.append("timestamp is null")
            
            if device_id is None or len(str(device_id).strip()) == 0:
                errors.append("device_id is null or empty")
            
            if not errors:
                return (True, None)
            else:
                return (False, "; ".join(errors))

        #create validation_flags
        validated_df = event_df.withColumn(
            "is_package_id_valid",
            when(
                col("package_id").isNotNull() & col("package_id").startswith("PKG-"),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "is_scan_type_valid",
            when(
                col("scant_type").isNotNull &
                col("scan_type").isin([
                    "picked", "shipped", "in_transit", "out_for_delivery", "delivered"
                ]),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "is_location_id_valid",
            when(
                col("location_id").isNotNull & (length(col("location_id")) > 0),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "is_timestamp_valid",
            when(
                col("event_time").isNotNull(),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "is_device_id_valid",
            when(
                col("device_id").isNotNull() & (length(col("devive_id")) > 0),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "is_valid",
            col("is_package_id_valid") & 
            col("is_scan_type_valid") & 
            col("is_location_id_valid") & 
            col("is_timestamp_valid") & 
            col("is_device_id_valid")
        ).withColumn(
            "validation_errors",
            when(
                ~col("is_package_id_valid"), 
                concat(lit("package_id invalid; "), when(~col("is_scan_type_valid"), lit("scan_type invalid; ")).otherwise(lit("")))
            ).when(
                ~col("is_scan_type_valid"),
                concat(lit("scan_type invalid; "), when(~col("is_location_id_valid"), lit("location_id invalid; ")).otherwise(lit("")))
            ).when(
                ~col("is_location_id_valid"),
                concat(lit("location_id invalid; "), when(~col("is_timestamp_valid"), lit("timestamp invalid; ")).otherwise(lit("")))
            ).when(
                ~col("is_timestamp_valid"),
                concat(lit("timestamp invalid; "), when(~col("is_device_id_valid"), lit("device_id invalid; ")).otherwise(lit("")))
            ).when(
                ~col("is_device_id_valid"),
                lit("device_id invalid")
            ).otherwise(lit(None))
        )

        return validated_df

    def read_from_kafka(self):
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.max.partition.fetch.bytes", "1048576") \
            .option("kafka.fetch.max.wait.ms", "1000") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()

    def parse_events(self,raw_df):

        parsed_df=raw_df\
            .select(
                from_json(
                    col("value").cast("string"),
                    self.input_schema,
                    {"mode": "PERMISSIVE"}
                ).alias("data"),
                col("offset"),
                col("partition"),
                col("timestamp").alias("kafka_timestamp")
            ) \
            .select(
                col("data.*"),
                col("offset"),
                col("partition"),
                col("kafka_timestamp")
            )
        #convert timestamp string to timestamp type
        parsed_df=parsed_df\
            .withColumn("event_timestamp",to_timestamp(col("event_timestamp")))\
            .withColumn("processing_timesstamp",current_timestamp())

        return parsed_df

    def split_valid_invalid(self,validated_df):

        valid_df=validated_df.filter(col("is_valid") == True)
        invalid_df=validated_df.filter(col("is_valid") == False)
        return valid_df,invalid_df

    def write_to_kafka(self,df,topic,output_mode='append'):
        return df.select(
            to_json(
                struct(
                    col("package_id"),
                    col("scan_type"),
                    col("location_id"),
                    col("timestamp"),
                    col("device_id"),
                    col("available_time"),
                    col("is_malformed"),
                    col("malformed_type")
                )
            ).alias("value"),
            col("package_id").alias("key")
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", f"{self.checkpoint_location}/kafka_{topic}") \
            .outputMode(output_mode) \
            .trigger(processingTime="5 seconds") \
            .option("kafka.acks", "all") \
            .option("kafka.retries", "3") \
            .option("kafka.compression.type", "snappy")

    def write_to_s3_partitioned(self,df):
        partitioned_df = df \
            .withColumn("year", year(col("event_timestamp"))) \
            .withColumn("month", month(col("event_timestamp"))) \
            .withColumn("day", dayofmonth(col("event_timestamp"))) \
            .withColumn("hour", hour(col("event_timestamp"))) \
            .withColumn("scan_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
        
        # Write to S3 with partitioning
        s3_path = f"{self.s3_bucket}/valid_scans"
        
        # Write to S3 with partition columns
        query = partitioned_df \
            .writeStream \
            .format("parquet") \
            .option("path", s3_path) \
            .option("checkpointLocation", f"{self.checkpoint_location}/s3_valid") \
            .partitionBy("year", "month", "day", "scan_type") \
            .outputMode("append") \
            .trigger(processingTime="1 minute") \
            .option("maxRecordsPerFile", "10000")
        
        return query

