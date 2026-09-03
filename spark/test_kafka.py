from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaMinimalTest") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
    .getOrCreate()

print("Session created")

try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:29092") \
        .option("subscribe", "package_events") \
        .load()
    print("SUCCESS: readStream loaded")
    print(df.schema)
except Exception as e:
    import traceback
    traceback.print_exc()