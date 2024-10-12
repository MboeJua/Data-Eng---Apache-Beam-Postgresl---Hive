import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PostgresToHive") \
    .config("spark.jars", "/Users/local/Downloads/postgresql-42.2.22.jar") \   # adjust to path of jdbc driver if postgresql source db
    .config("hive.metastore.uris", "thrift://localhost:9083") \   #adjust port based on preference
    .enableHiveSupport() \
    .getOrCreate()

# Define JDBC options for PostgreSQL
jdbc_url = "jdbc:postgresql://host.docker.internal:5435/postgres"
jdbc_properties = {
    "user": "db_user",
    "password": "db_password",
    "driver": "org.postgresql.Driver"
}

# Path to store the last processed timestamp
timestamp_file_path = '/path/to/last_processed_timestamp.txt'

# Handle first run by checking if the timestamp file exists
if not os.path.exists(timestamp_file_path):
    # If the file doesn't exist, assume it's the first run and process all records
    print("First run detected, processing all records.")
    last_processed_timestamp = '2024-09-09 00:00:00'  # Adjust default timestamp
else:
    # Read the last processed timestamp from the file
    with open(timestamp_file_path, 'r') as file:
        last_processed_timestamp = file.read().strip()

# Read data from PostgreSQL
df_postgres = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT id, created_at FROM time_table WHERE created_at > '{}') as time_table".format(last_processed_timestamp),
    properties=jdbc_properties
)

# Filter new rows based on the last processed timestamp
df_filtered = df_postgres.filter(col("created_at") > last_processed_timestamp)

# Optionally convert `created_at` to a Spark TimestampType if necessary
df_filtered = df_filtered.withColumn("created_at", F.to_timestamp("created_at"))

# Write data to Parquet format to be used by Hive
output_path = "/usr/local/hadoop/parquet_output"    #adjust based on preferred path to store parquet
df_filtered.write.mode("overwrite").parquet(output_path)

# Create or update an external Hive table with the new data
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS time_table_spark (
    id BIGINT,
    created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION '{}'
""".format(output_path))

# Update the last processed timestamp (write the latest timestamp to the file)
latest_timestamp = df_filtered.agg({"created_at": "max"}).collect()[0][0]
if latest_timestamp:
    with open(timestamp_file_path, 'w') as file:
        file.write(latest_timestamp)

# Stop the Spark session
spark.stop()

