import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    unix_timestamp,
    sum as _sum,
    date_format,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)
import great_expectations as ge


# Ensure results directory exists
os.makedirs("results", exist_ok=True)

# Spark Session
spark = (
    SparkSession.builder
    .appName("BikeRideDuration")
    .master("local[*]")
    .getOrCreate()
)

# Define Schema
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True),
])

# Load CSV
df = spark.read.csv(
    "data/202306-divvy-tripdata.csv",
    header=True,
    schema=schema,
    mode="DROPMALFORMED"
)

# Convert timestamps
df = df.withColumn(
    "started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss")
)

# Compute duration
df = df.withColumn(
    "duration_seconds",
    unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))
)

# Add date column
df = df.withColumn(
    "date", date_format(col("started_at"), "yyyy-MM-dd")
)

# Add duration status column
df = df.withColumn(
    "duration_status",
    when(col("duration_seconds") > 86400, "invalid").otherwise("valid")
)

# Great Expectations Validation
gdf = ge.dataset.SparkDFDataset(df)

gdf.expect_column_values_to_not_be_null("started_at")
gdf.expect_column_values_to_not_be_null("ended_at")
gdf.expect_column_values_to_be_between(
    "duration_seconds", min_value=0, max_value=86400
)

validation_results = gdf.validate()

if not validation_results["success"]:
    print("Warning: Invalid trip durations detected.")
else:
    print("Data quality check passed.")

# Print invalid rows in readable table format
print("\nTrips with duration > 24 hours:\n")
df.filter(col("duration_status") == "invalid").show(n=1000, truncate=False)

# Output paths
parquet_path = "results/final_output.parquet"
csv_path = "results/final_output.csv"

# Write Parquet output
df.write.mode("overwrite").parquet(parquet_path)

# Write CSV as a single real file (not a folder)
temp_csv_path = "results/temp_csv"

df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_csv_path)

# Move part file to final_output.csv
for file in os.listdir(temp_csv_path):
    if file.endswith(".csv"):
        shutil.move(
            os.path.join(temp_csv_path, file),
            csv_path
        )

# Delete temp folder
shutil.rmtree(temp_csv_path)

print(f"Final dataset written to: {parquet_path}")
print(f"Final dataset written to: {csv_path}")

spark.stop()
