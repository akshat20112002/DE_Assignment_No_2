import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    unix_timestamp,
    sum as _sum,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)
import great_expectations as ge


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

# Transform Data: parse timestamps and compute duration
df = df.withColumn(
    "started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss")
)

df = df.withColumn(
    "duration_seconds",
    unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))
)

df = df.withColumn(
    "date", date_format(col("started_at"), "yyyy-MM-dd")
)

# Great Expectations Validation (GE 0.17.x API)
gdf = ge.dataset.SparkDFDataset(df)

# Expectations
gdf.expect_column_values_to_not_be_null("started_at")
gdf.expect_column_values_to_not_be_null("ended_at")

# Duration must be within same day (0 to 24 hours)
gdf.expect_column_values_to_be_between(
    "duration_seconds",
    min_value=0,
    max_value=86400
)

# Run validation
results = gdf.validate()

# Fail pipeline if validation fails
if not results["success"]:
    print("\nDATA QUALITY FAILED: Invalid trip durations found.\n")
    raise ValueError("Data quality validation failed.")
else:
    print("\nDATA QUALITY PASSED.\n")

# Aggregation
daily = df.groupBy("date").agg(
    _sum("duration_seconds").alias("total_duration_seconds")
)

output_path = "results/output_file.parquet"
daily.write.mode("overwrite").parquet(output_path)

print(f"Output written to: {output_path}")

spark.stop()
