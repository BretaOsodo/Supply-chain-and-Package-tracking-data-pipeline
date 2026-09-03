import json 
import logging 
import math 
import random
import builtins
from datetime import datetime,timedelta,timezone
from typing import Dict, Optional, Tuple,Iterator,List
from dataclasses import dataclass, asdict 
from dotenv import load_dotenv

from config import config

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, from_json, struct, to_json, lit, when,
    current_timestamp, explode, collect_list, array
)
from pyspark.sql.streaming import StreamingQueryListener 

import redis
import os 
import boto3


logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

load_dotenv()

# Data Models 
@dataclass 
class ETAcalculation:
    """ETA calculation result"""
    package_id:str
    current_state:str
    current_location:str
    predicted_delivery_time:str
    eta_hours: float
    confidence_score: float
    estimated_route: List[str]
    time_factors: Dict[str,float]
    calculation_timestamp:str

    def to_dict(self)-> Dict:
        return asdict(self)

    def to_json(self)-> str:
        return json.dumps(self.to_dict(),default=str)

@dataclass
class LocationInfo:
    """Location information for ETA calculation"""
    location_id:str
    city_code: str
    city_name:str
    zone: str
    base_time: float
    coordinates: Optional[Tuple[float,float]]=None

#Eta calculator class 
class ETAcalculator:
    def __init__(
            self,
            bootstrap_servers:str="kafka1:29092,kafka2:29092",
            input_topic:str="state-processed-data",
            output_topic="eta-calculated-data",
            redis_host:str="127.0.0.1",
            redis_port:int=6379,
            redis_db:int=0,
            redis_password= os.getenv("REDIS_PASSWORD"),
            checkpoint_location:str="/spark/checkpoints/eta_calculator",
            spark_config:dict=None

    ):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic=output_topic
        self.redis_host=redis_host
        self.redis_port=redis_port
        self.redis_password=redis_password
        self.redis_db=redis_db
        self.checkpoint_location = checkpoint_location
        self.spark_config = spark_config

        #initialize spark 
        self.spark = self._create_spark_session(spark_config)

        #define schema 

        self.input_schema = self._create_input_schema()
        self.eta_schema = self._create_eta_schema()

        #initialize redis client 
        self._redis_client = None 

        #location cache 
        self.location_cache= self._build_location_cache()

    def _create_spark_session(self,spark_config: dict=None)-> SparkSession:
        """create spark session"""
        builder= SparkSession.builder\
                .appName("eta calculator")\
                .config("spark.sql.shuffle.partitions","8")\
                .config("spark.sql.streaming.schemaInference", "false") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.session.timeZone", "UTC")
        if spark_config:
            for key, value in spark_config.items():
                builder= builder.config(key,value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _create_input_schema(self)->StructType:
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

    def _create_eta_schema(self)->StructType:
        return StructType([
            StructField("package_id", StringType(), False),
            StructField("current_state", StringType(), False),
            StructField("current_location", StringType(), True),
            StructField("predicted_delivery_time", StringType(), True),
            StructField("eta_hours", DoubleType(), True),
            StructField("confidence_score", DoubleType(), True),
            StructField("estimated_route", ArrayType(StringType()), True),
            StructField("time_factors", MapType(StringType(), DoubleType()), True),
            StructField("calculation_timestamp", StringType(), True)
        ])

    def _build_location_cache(self)-> Dict[str,LocationInfo]:
        cache={}
        for code , info in config.KENYA_LOCATIONS.items():
            cache[code]=LocationInfo(
                location_id=code,
                city_code=code,
                city_name=info["city"],
                zone=info["zone"],
                base_time = info["base_time"]
            )
        return cache

    def _get_redis_client(self):
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
                self._redis_client.ping()
                logger.info(f"Connecter to redist at {self.redis_host}:{self.redis_port}")
            except Exception as e:
                logger.error(f"Failed to connect to redis: {e}")
                self._redis_client = None
        return self._redis_client

    def _parse_input(self, df):
        return df.select(
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

    def _extract_location_info(self, location_id:str)->LocationInfo:
        if not location_id:
            return LocationInfo(
                location_id="unknown",
                city_code="unknown",
                city_name="unknown",
                zone="urban",
                base_time=2.0
            )

        #check if location_id matches kenya pattern
        #extract the city code
        parts=location_id.split("-")
        if parts:
            city_code=parts[0]
            if city_code in self.location_cache:
                return self.location_cache[city_code]

        #check for TRUCK or ZONE patterns 
        if location_id.startswith("TRUCK"):
            return LocationInfo(
                location_id=location_id,
                city_code="Truck",
                city_name="In Transit",
                zone="urban",
                base_time=0.5)
        elif location_id.startswith("RESIDENTIAL"):
            return LocationInfo(
                location_id=location_id,
                city_code="DELIVERY",
                city_name="Delivery Zone",
                zone="urban",
                base_time=0.5
            )

        #default fallback 
        return LocationInfo(
            location_id=location_id,
            city_code='DEFAULT',
            city_name="Default",
            zone= "urban",
            base_time=2.0
        )

    def _calculate_eta(self,package_state:Dict)->Dict:
        package_id = package_state.get("package_id")
        current_state = package_state.get("current_state")
        current_location = package_state.get("current_location")
        state_history = package_state.get("state_history", [])
        first_scan_time = package_state.get("first_scan_time")
        last_scan_time = package_state.get("last_scan_time")
        total_scans = package_state.get("total_scans", 0)
        completed = package_state.get("completed", False)


        #if already delivered, ETA is in the past 
        if completed:
             return {
                "package_id": package_id,
                "current_state": "delivered",
                "current_location": current_location,
                "predicted_delivery_time": package_state.get("completion_time"),
                "eta_hours": 0.0,
                "confidence_score": 1.0,
                "estimated_route": state_history,
                "time_factors": {"completed": 1.0},
                "calculation_timestamp": datetime.now(timezone.utc).isoformat()
            }

        #extract location info 
        loc_info=self._extract_location_info(current_location)

        #calculate base Eta 
        base_time= self._calculate_base_time(loc_info, current_state,total_scans)

        #apply state multiplier 
        state_multiplier=config.STATE_TIME_MULTIPLIERS.get(current_state,1.0)

        #apply zone adjustement
        zone_adjustment= config.ZONE_TIME_ADJUSTMENTS.get(loc_info.zone,1.0)

        #apply peak hour adjustment 
        peak_adjustment =self._calculate_peak_hour_adjustment(last_scan_time)

        #apply scan history adjustement 
        history_adjustment= builtins.min(1+(total_scans-1)* 0.05 , 1.5)

        #calculaye final ETA hours 
        eta_hours= base_time * state_multiplier*zone_adjustment*peak_adjustment/history_adjustment

        #calculate confidence score based on data Quality 
        confidence_score = self._calculate_confidence(
            total_scans,
            len(state_history),
            current_state,
            loc_info.zone
        )

        #predict delivery time 
        try:
            last_time = datetime.fromisoformat(last_scan_time.replace('Z', '+00:00'))

        except:
            last_time= datetime.now(timezone.utc)

        predicted_delivery= last_time + timedelta(hours=eta_hours)

        #build route estimation 
        estimated_route = state_history.copy()
        if current_state != "delivered":
            estimated_route.append("delivered")

        #build time factor breakdown 
        time_factors = {
            "base_time": base_time,
            "state_multiplier": state_multiplier,
            "zone_adjustment": zone_adjustment,
            "peak_adjustment": peak_adjustment,
            "history_adjustment": history_adjustment
        }
        return {
            "package_id": package_id,
            "current_state": current_state,
            "current_location": current_location,
            "predicted_delivery_time": predicted_delivery.isoformat().replace("+00.00","Z"),
            "eta_hours": builtins.round(eta_hours, 2),
            "confidence_score": builtins.round(confidence_score, 3),
            "estimated_route": estimated_route,
            "time_factors": time_factors,
            "calculation_timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _calculate_base_time(self,loc_info: LocationInfo,state:str,total_scans:int)-> float:

        #base transit time from location 
        base = loc_info.base_time

        # Adjust based on state
        if state == "picked":
            # Still at warehouse, add processing and transit
            base += 4.0
        elif state == "shipped":
            # At hub, add hub processing + transit
            base += 3.0
        elif state == "in_transit":
            # On the road, remaining transit
            base = builtins.max(base * 0.5, 1.0)
        elif state == "out_for_delivery":
            # Almost there!
            base = builtins.max(base * 0.2, 0.5)

        #Add random variation
        variation=1.0 + (random.random() -0.5)*0.3
        base *= variation 

        return builtins.max(base,0.5) #minimum 30 minutes 

    def _calculate_peak_hour_adjustment(self,last_scan_time:str)-> float:
        try:
            if last_scan_time:
                dt = datetime.fromisoformat(last_scan_time.replace('Z', '+00:00'))
                hour = dt.hour
                
                # Check if in peak hours (Kenya time)
                if config.PEAK_HOURS["morning"][0] <= hour <= config.PEAK_HOURS["morning"][1]:
                    return 1.3  # 30% slower in morning peak
                elif config.PEAK_HOURS["evening"][0] <= hour <=config.PEAK_HOURS["evening"][1]:
                    return 1.4  # 40% slower in evening peak
        except:
            pass
        
        return 1.0

    def _calculate_confidence(self,total_scans:int, history_length:int,state:str,zone:str)-> float:
        # More scans = higher confidence
        scan_confidence = builtins.min(total_scans / 5.0, 1.0)
        
        # History length confidence
        history_confidence = builtins.min(history_length / 3.0, 1.0)
        
        # State confidence
        state_confidence = {
            "picked": 0.4,
            "shipped": 0.6,
            "in_transit": 0.7,
            "out_for_delivery": 0.9,
            "delivered": 1.0
        }.get(state, 0.5)
        
        # Zone confidence (some zones are less predictable)
        zone_confidence = {
            "urban": 0.9,
            "coastal": 0.8,
            "highland": 0.7,
            "lake": 0.75,
            "arid": 0.6
        }.get(zone, 0.7)
        
        # Weighted average
        confidence = (
            scan_confidence * 0.3 +
            history_confidence * 0.2 +
            state_confidence * 0.3 +
            zone_confidence * 0.2
        )
        
        return builtins.min(confidence, 1.0)

    def _write_to_redis_partition(self,iterator: Iterator[Dict])->None:
        import redis
        
        try:
            redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
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
        
        for eta_data in iterator:
            try:
                package_id = eta_data.get("package_id")
                if not package_id:
                    continue
                
                # Store full ETA data as JSON
                eta_json = json.dumps(eta_data, default=str)
                pipeline.setex(
                    f"eta:package:{package_id}",
                    86400 * 7,  # 7 days
                    eta_json
                )
                
                # Store individual fields for easy querying
                pipeline.hset(
                    f"eta:package:{package_id}:fields",
                    mapping={
                        "current_state": eta_data.get("current_state", ""),
                        "eta_hours": str(eta_data.get("eta_hours", 0)),
                        "predicted_delivery_time": eta_data.get("predicted_delivery_time", ""),
                        "confidence_score": str(eta_data.get("confidence_score", 0)),
                        "current_location": eta_data.get("current_location", ""),
                        "calculation_timestamp": eta_data.get("calculation_timestamp", "")
                    }
                )
                
                # Add to ETA index sets
                pipeline.sadd("eta:all", package_id)
                
                # Add to state-specific index
                state = eta_data.get("current_state", "unknown")
                pipeline.sadd(f"eta:state:{state}", package_id)
                
                # Add to zone index
                time_factors = eta_data.get("time_factors", {})
                zone = time_factors.get("zone", "unknown")
                pipeline.sadd(f"eta:zone:{zone}", package_id)
                
                # Add to confidence tiers
                confidence = eta_data.get("confidence_score", 0)
                if confidence >= 0.8:
                    pipeline.sadd("eta:confidence:high", package_id)
                elif confidence >= 0.5:
                    pipeline.sadd("eta:confidence:medium", package_id)
                else:
                    pipeline.sadd("eta:confidence:low", package_id)
                
                batch_count += 1
                if batch_count >= 100:
                    pipeline.execute()
                    pipeline = redis_client.pipeline()
                    batch_count = 0
                    
            except Exception as e:
                logger.error(f"Failed to write ETA for {package_id}: {e}")
        
        if batch_count > 0:
            try:
                pipeline.execute()
            except Exception as e:
                logger.error(f"Failed to execute final Redis pipeline: {e}")

    def process_stream(self):
        """Process streaming data and calculate ETA."""
        logger.info("Starting ETA Calculator Pipeline...")
        logger.info(f"Input topic: {self.input_topic}")
        logger.info(f"Output topic: {self.output_topic}")
        logger.info(f"Redis: {self.redis_host}:{self.redis_port}")
        logger.info(f"Using Kenya-specific location data with {len(config.KENYA_LOCATIONS)} locations")
        
        # Read from Kafka
        raw_stream = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.input_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()
        
        # Parse input
        parsed_stream = self._parse_input(raw_stream)
        
        # Define UDF for ETA calculation
        def process_batch(df, batch_id):
            if df.isEmpty():
                return
            
            # Calculate on driver
            rows = df.collect()
            eta_results = [self._calculate_eta(row.asDict()) for row in rows]
            
            if not eta_results:
                return
            
            eta_df = self.spark.createDataFrame(eta_results, schema=self.eta_schema)
            
            # Write to Kafka
            kafka_df = eta_df.select(
                to_json(struct("*")).alias("value"),
                col("package_id").alias("key")
            )
            kafka_df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("topic", self.output_topic) \
                .option("kafka.acks", "all") \
                .mode("append") \
                .save()
            
            # Write to Redis (extract variables first)
            redis_host = self.redis_host
            redis_port = self.redis_port
            redis_db = self.redis_db
            redis_password = self.redis_password

            def write_to_redis(eta_results_list):
                try:
                    client = redis.Redis(
                        host=self.redis_host,
                        port=self.redis_port,
                        db=self.redis_db,
                        password=self.redis_password,
                        decode_responses=True,
                        socket_connect_timeout=5,
                        socket_timeout=5
                    )
                    client.ping()
                except Exception as e:
                    logger.error(f"Redis connect failed: {e}")
                    return
                
                pipe = client.pipeline()
                count = 0
                
                for eta_data in eta_results_list:
                    try:
                        pkg_id = eta_data["package_id"]
                        
                        # Store full ETA JSON
                        pipe.setex(f"eta:package:{pkg_id}", 86400 * 7, json.dumps(eta_data))
                        
                        # Store hash fields
                        pipe.hset(f"eta:package:{pkg_id}:fields", mapping={
                            "current_state": eta_data.get("current_state", ""),
                            "eta_hours": str(eta_data.get("eta_hours", 0)),
                            "predicted_delivery_time": eta_data.get("predicted_delivery_time", ""),
                            "confidence_score": str(eta_data.get("confidence_score", 0)),
                            "current_location": eta_data.get("current_location", ""),
                            "calculation_timestamp": eta_data.get("calculation_timestamp", "")
                        })
                        
                        # Indexes
                        state = eta_data.get("current_state", "unknown")
                        zone = eta_data.get("time_factors", {}).get("zone", "unknown")
                        confidence = eta_data.get("confidence_score", 0)
                        
                        pipe.sadd("eta:all", pkg_id)
                        pipe.sadd(f"eta:state:{state}", pkg_id)
                        pipe.sadd(f"eta:zone:{zone}", pkg_id)
                        
                        if confidence >= 0.8:
                            pipe.sadd("eta:confidence:high", pkg_id)
                        elif confidence >= 0.5:
                            pipe.sadd("eta:confidence:medium", pkg_id)
                        else:
                            pipe.sadd("eta:confidence:low", pkg_id)
                        
                        count += 1
                        if count >= 100:
                            pipe.execute()
                            pipe = client.pipeline()
                            count = 0
                            
                    except Exception as e:
                        logger.error(f"Failed to write ETA for {pkg_id}: {e}")
                
                if count > 0:
                    try:
                        pipe.execute()
                    except Exception as e:
                        logger.error(f"Final Redis pipeline failed: {e}")
                
                client.close()
            
            # Call it directly with the list
            write_to_redis(eta_results)

        query = parsed_stream.writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", self.checkpoint_location) \
            .trigger(processingTime="10 seconds") \
            .queryName("eta-calculator") \
            .start()
        return query
            
    def run_pipeline(self):
        """Run the complete ETA calculator pipeline."""
        query = self.process_stream()
        
        logger.info("ETA Calculator pipelines started. Waiting for termination...")
        
        # Wait for both queries
        try:
            # Wait for kafka query
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping ETA Calculator...")
            query.stop()
            logger.info("ETA Calculator stopped")

class StreamingQueryMonitor(StreamingQueryListener):
    """Monitor streaming queries."""
    
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



# Main


def main():
    """Main entry point."""
    
    # Configuration
    KAFKA_BROKER = "kafka1:29092,kafka2:29092"
    INPUT_TOPIC = "state-processed-data"
    OUTPUT_TOPIC = "eta-calculated-data"
    
    REDIS_HOST = "redis"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = None
    
    CHECKPOINT_LOCATION = "/spark/checkpoints/eta_calculator"
    
    spark_config = {
        "spark.sql.streaming.stateStore.providerClass":
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        "spark.sql.streaming.stateStore.compression.codec": "lz4",
        "spark.sql.streaming.noDataProgressEventInterval": "10s"
    }
    
    # Initialize calculator
    calculator = ETAcalculator(
        bootstrap_servers=KAFKA_BROKER,
        input_topic=INPUT_TOPIC,
        output_topic=OUTPUT_TOPIC,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        redis_db=REDIS_DB,
        redis_password=REDIS_PASSWORD,
        checkpoint_location=CHECKPOINT_LOCATION,
        spark_config=spark_config
    )
    
    # Add listener
    monitor = StreamingQueryMonitor()
    calculator.spark.streams.addListener(monitor)
    
    try:
        calculator.run_pipeline()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise


if __name__ == "__main__":
    main()