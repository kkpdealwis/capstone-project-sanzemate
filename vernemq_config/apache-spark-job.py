
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("data", StructType([
        StructField("t", DoubleType(), True),
        StructField("h", DoubleType(), True),
        StructField("r", IntegerType(), True),
        StructField("f", IntegerType(), True),
        StructField("u", IntegerType(), True),
        StructField("s", IntegerType(), True),
        StructField("c", IntegerType(), True),
        StructField("c_id", IntegerType(), True),
        StructField("c_fq", IntegerType(), True),
        StructField("cpu_usage", DoubleType(), True),
        StructField("memory_usage", DoubleType(), True),
        StructField("disk_usage", DoubleType(), True),
        StructField("network_stats", StructType([
            StructField("bytes_sent", LongType(), True),
            StructField("bytes_recv", LongType(), True),
            StructField("packets_sent", LongType(), True),
            StructField("packets_recv", LongType(), True),
            StructField("errin", IntegerType(), True),
            StructField("errout", IntegerType(), True),
            StructField("dropin", IntegerType(), True),
            StructField("dropout", IntegerType(), True),
        ]), True),
    ]), True)
])

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.105:9092") \
    .option("subscribe", "logs") \
    .load()

# Extract value and cast to string
value_df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON and create structured DataFrame
json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Calculate averages using a sliding window of 10 minutes with a slide duration of 5 minutes
windowed_avg = json_df.groupBy(
    window(col("timestamp").cast("timestamp"), "10 minutes", "1 minutes"),
    col("data.r").alias("nodemcu_rssi"),
    col("data.f").alias("nodemcu_freeheap"),
    col("data.u").alias("nodemcu_usedheap"),
    col("data.s").alias("nodemcu_sketch_size"),
    col("data.c").alias("nodemcu_flash_chip_size"),
    col("data.c_id").alias("nodemcu_chip_id"),
    col("data.c_fq").alias("nodemcu_cpu_frequency")
).agg(
    avg("data.h").alias("avg_humidity"),
    avg("data.t").alias("avg_temperature"),
    avg("data.cpu_usage").alias("avg_cpu_usage"),
    avg("data.memory_usage").alias("avg_memory_usage"),
    avg("data.disk_usage").alias("avg_disk_usage")
)

# Convert the windowed_avg DataFrame to JSON format
output_df = windowed_avg.selectExpr(
    "to_json(struct(*)) AS value"
)

# Write the result to a new Kafka topic
query = output_df.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.105:9092") \
    .option("topic", "avg_monitoring_values") \
    .option("checkpointLocation", "/opt/spark-apps/spark-checkpoints/") \
    .start()

query.awaitTermination()
