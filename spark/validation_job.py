from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from typing import Dict
import time 
import logging 
import os 
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS= "kafka1:29092,kafka2:29092"
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
        self.s3_bucket= (
            s3_bucket
            if s3_bucket.startswith("s3a://")
            else f"s3a://{s3_bucket}"
        )
        self.checkpoint_location=checkpoint_location


        #initalize spark session with s3 support 
        self.spark = self._create_spark_session(spark_config)

        #define schema
        self.input_schema = self._create_input_schema()
        self.validation_results_schema= self._create_validation_schema()

        #validation rules
        self.validation_rules=self._create_validation_rules()


    def _create_spark_session(self, spark_config: dict = None) -> SparkSession:
        builder = SparkSession.builder \
            .appName("PackageTrackingDataValidation") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.streaming.stateStore.providerClass",
                    "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
            .config("spark.sql.streaming.schemaInference", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.kafka.maxRatePerPartition", "1000") \
            .config("spark.sql.streaming.backpressure.enabled", "true") \
            .config("spark.sql.streaming.backpressure.initialRate", "100") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")

        builder = builder \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
            .config("spark.hadoop.fs.s3a.max.total.tasks", "5")

        if spark_config:
            for key, value in spark_config.items():
                builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        # Diagnostic: confirm Kafka provider is reachable
        try:
            spark._jvm.org.apache.spark.sql.kafka010.KafkaSourceProvider
            logger.info("Kafka connector verified on classpath")
        except Exception as e:
            logger.error("Kafka connector NOT on classpath: %s", e)

        return spark

    def _create_input_schema(self)->StructType:

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
                col("scan_type").isNotNull() &
                col("scan_type").isin([
                    "picked", "shipped", "in_transit", "out_for_delivery", "delivered"
                ]),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "is_location_id_valid",
            when(
                col("location_id").isNotNull() & (length(col("location_id")) > 0),
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
                col("device_id").isNotNull() & (length(col("device_id")) > 0),
                lit(True)
            ).otherwise(lit(False))
        ).withColumn(
            "is_valid",
            col("is_package_id_valid") & 
            col("is_scan_type_valid") & 
            col("is_location_id_valid") & 
            col("is_timestamp_valid") & 
            col("is_device_id_valid") &
            (~coalesce(col("is_malformed"), lit(False)))
        ).withColumn(
    "validation_errors",
    concat_ws(
        "; ",
        when(
            ~col("is_package_id_valid"),
            lit("package_id invalid")
        ),
        when(
            ~col("is_scan_type_valid"),
            lit("scan_type invalid")
        ),
        when(
            ~col("is_location_id_valid"),
            lit("location_id invalid")
        ),
        when(
            ~col("is_timestamp_valid"),
            lit("event_time invalid")
        ),
        when(
            ~col("is_device_id_valid"),
            lit("device_id invalid")
        ),
        when(
            coalesce(col("is_malformed"), lit(False)),
            concat(
                lit("malformed event: "),
                coalesce(
                    col("malformed_type"),
                    lit("unknown")
                )
            )
        )
    )
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
            .withColumn(
                "event_time",
                to_timestamp(
                    col("event_time"),
                    "yyyy-MM-dd'T'HH:mm:ss'Z'"
                )
            )\
            .withColumn("processing_timestamp",current_timestamp())

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
                    col("event_time"),
                    col("available_time"),
                    col("device_id"),
                    col("is_malformed"),
                    col("malformed_type"),
                    col("is_valid"),
                    col("validation_errors"),
                    col("kafka_timestamp"),
                    col("processing_timestamp")
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
            .withColumn("year", year(col("event_time"))) \
            .withColumn("month", month(col("event_time"))) \
            .withColumn("day", dayofmonth(col("event_time"))) \
            .withColumn("hour", hour(col("event_time"))) \
            .withColumn("scan_date", date_format(col("event_time"), "yyyy-MM-dd"))
        
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

    def run_pipeline(self):
        """
        Run the complete streaming pipeline.
        """

        logger.info("Starting Package Tracking Validation Pipeline...")
        logger.info(f"Input topic: {self.input_topic}")
        logger.info(f"Validated topic: {self.validated_topic}")
        logger.info(f"Invalidated topic: {self.invalidated_topic}")
        logger.info(f"S3 bucket: {self.s3_bucket}")
        logger.info(f"Checkpoint location: {self.checkpoint_location}")

        #read from kafka
        raw_stream = self.read_from_kafka()

        #parse events 
        parsed_stream=self.parse_events(raw_stream)

        #validated events 
        validated_stream = self.validate_event(parsed_stream)

        #split streams 
        valid_stream, invalid_stream=self.split_valid_invalid(validated_stream)

        #define output query

        queries=[]

        #1. Write valid data to kafka validated topic
        if self.validated_topic:
            logger.info(
                f"Writing data to kafka topic: {self.validated_topic}"
            )

            valid_kafka_query=self.write_to_kafka(
                valid_stream,
                self.validated_topic
            ).start()

            queries.append(valid_kafka_query)

        #2. write invalid data to kafka invalidated topic
        if self.invalidated_topic:
            logger.info(f"Writing invalid data to kafka topic:{self.invalidated_topic}")

            invalid_kafka_query= self.write_to_kafka(
                invalid_stream,
                self.invalidated_topic
            ).start()
            queries.append(invalid_kafka_query)

        #. write valid data to s3 with partitioning
        if self.s3_bucket:
            logger.info(f"Writing valid data to s3: {self.s3_bucket}")
            s3_query=self.write_to_s3_partitioned(valid_stream).start()
            queries.append(s3_query)

        #wait for all queries to finish 
        for query in queries:
            query.awaitTermination()

def main():
    #initialize pipeline
    pipeline=ValidationPipeline(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        input_topic=KAFKA_INPUT_TOPIC,
        validated_topic=KAFKA_VALIDATED_TOPIC,
        invalidated_topic=KAFKA_INVALIDATED_TOPIC,
        s3_bucket=S3_BUCKET_VALIDATED,
        checkpoint_location=CHECKPOINT_LOCATION,

    )

    try:
        #run pipeline
        pipeline.run_pipeline()

    except KeyboardInterrupt:
        logger.info("pipeline stopped by user")

    except Exception as e:
        logger.error(f"Pipeline error")

        raise 

if __name__=="__main__":
    main()