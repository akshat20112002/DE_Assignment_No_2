from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path

def add_source_file_col(df, zip_path):
    file_name = Path(Path(zip_path).stem).stem
    return df.withColumn("source_file", F.lit(file_name))

def add_file_date_col(df):
    return df.withColumn("file_date", F.to_date(F.regexp_extract(F.col("source_file"), r"(\d{4}-\d{2}-\d{2})", 1), "yyyy-MM-dd"))

def add_brand_col(df):
    return df.withColumn("brand",F.when(F.col("model").contains(" "), F.split(F.col("model"), " ").getItem(0)).otherwise("unknown"))

def add_storage_ranking(df):
    df2 = df.withColumn("capacity_bytes", F.col("capacity_bytes").cast("bigint"))
    w = Window.partitionBy("brand").orderBy(F.col("max_capacity").desc())
    sec_df = (df2.groupBy("model", "brand").agg(F.max("capacity_bytes").alias("max_capacity")).withColumn("storage_ranking", F.dense_rank().over(w)))

    return df2.join(sec_df.select("model","storage_ranking"), "model", "left")


def add_primary_key(df):
    return df.withColumn("primary_key", F.sha2(F.concat_ws("||", "date", "serial_number"), 256))