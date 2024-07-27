from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, window, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark to Elasticsearch Example") \
    .config("spark.es.nodes", "192.168.1.105") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# Load sample data
data_path = "/opt/spark-apps/sensor_data.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Convert timestamp column to timestamp type
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Process data (calculate average temperature and humidity over 1 minute window)
windowed_df = df \
    .groupBy(window(col("timestamp"), "1 minute"), col("sensor_id")) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

# Show the processed data in the console
windowed_df.show(truncate=False)

# Write the processed data to Elasticsearch
windowed_df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "sensor-data-static/avg") \
    .save()

# Stop Spark session
spark.stop()
