from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Initialize Spark session with Elasticsearch package
spark = SparkSession.builder \
    .appName("KafkaToElasticsearch") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-hadoop:7.17.22") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("window", StructType([
        StructField("start", StringType(), True),
        StructField("end", StringType(), True)
    ]), True),
    StructField("avg_humidity", DoubleType(), True),
    StructField("avg_temperature", DoubleType(), True),
    StructField("avg_cpu_usage", DoubleType(), True),
    StructField("avg_memory_usage", DoubleType(), True),
    StructField("avg_disk_usage", DoubleType(), True),
    StructField("nodemcu_rssi", IntegerType(), True),
    StructField("nodemcu_freeheap", IntegerType(), True),
    StructField("nodemcu_usedheap", IntegerType(), True),
    StructField("nodemcu_sketch_size", IntegerType(), True),
    StructField("nodemcu_flash_chip_size", IntegerType(), True),
    StructField("nodemcu_chip_id", IntegerType(), True),
    StructField("nodemcu_cpu_frequency", IntegerType(), True)
])

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.105:9092") \
    .option("subscribe", "avg_monitoring_values") \
    .load()

# Extract value and cast to string
value_df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON and create structured DataFrame
json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Write the result to Elasticsearch
query = json_df.writeStream \
    .outputMode("append") \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "192.168.1.105") \
    .option("es.port", "9200") \
    .option("checkpointLocation", "/opt/spark-apps/spark-checkpoints/es") \
    .option("es.resource", "average-index/_doc") \
    .start()

query.awaitTermination()
