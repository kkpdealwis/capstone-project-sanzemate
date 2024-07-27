from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkApp") \
    .config("spark.es.nodes", "192.168.1.105") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# Define the Kafka parameters
kafka_bootstrap_servers = "192.168.1.105:9092"
kafka_topic = "logs"

# Define the schema for the JSON data
json_schema = StructType([
    StructField("level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("data", StructType([
        StructField("t", DoubleType(), True),  # temperature
        StructField("h", DoubleType(), True),  # humidity
        StructField("r", IntegerType(), True),
        StructField("f", IntegerType(), True),
        StructField("u", IntegerType(), True),
        StructField("s", IntegerType(), True),
        StructField("c", IntegerType(), True),
        StructField("c_id", IntegerType(), True),
        StructField("c_fq", IntegerType(), True),
        StructField("cpu_usage", DoubleType(), True),  # CPU usage
        StructField("memory_usage", DoubleType(), True),  # Memory usage
        StructField("disk_usage", DoubleType(), True),  # Disk usage
        StructField("network_stats", StructType([
            StructField("bytes_sent", LongType(), True),
            StructField("bytes_recv", LongType(), True),
            StructField("packets_sent", LongType(), True),
            StructField("packets_recv", LongType(), True),
            StructField("errin", IntegerType(), True),
            StructField("errout", IntegerType(), True),
            StructField("dropin", IntegerType(), True),
            StructField("dropout", IntegerType(), True)
        ]), True)
    ]), True)
])

# Create a DataFrame representing the stream of input lines from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Extract the JSON payload and apply the schema
parsed_df = kafka_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*")

# Define the window operation for 1 minute
windowed_df = parsed_df \
    .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp")) \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .agg(
        avg("data.h").alias("avg_humidity"),
        avg("data.t").alias("avg_temperature"),
        avg("data.cpu_usage").alias("avg_cpu_usage"),
        avg("data.memory_usage").alias("avg_memory_usage"),
        avg("data.disk_usage").alias("avg_disk_usage")
    )

# Write the results to Elasticsearch
es_query = windowed_df.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/opt/spark-apps/spark-checkpoints/es") \
    .option("es.resource", "spark-logs-avg/avg") \
    .option("es.nodes", "192.168.1.105") \
    .start()

# Write the results to the console
console_query = windowed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()
# Await termination of the query
es_query.awaitTermination()
console_query.awaitTermination()
