import json 
import logging 
import math 
from datetime import datetime,timedelta,timezone
from typing import Dict, Optional, Tuple,Iterator,List
from dataclasses import dataclass, asdict 
from dotenv import load_dotenv

from config import config

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQueryListener 

import redis
import os 
import boto3


logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

load_dotenv()

# Data Models 
@dataclass 
class ETAcalculator:
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
        return json.dumps(self.to_dict,default=str)

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
        self.redi_password=redis_password
        self.redis_database=redis_db
        self.checkpoint_location = checkpoint_location
        self.spark_config = spark_config

        #initialize spark 
        self.spark = self._create_spark_sessions(spark_config)

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