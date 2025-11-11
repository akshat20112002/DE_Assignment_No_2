from pyspark.sql import SparkSession
from source.extract import load_csv_from_zip
from source.utils import save_step
from source.transform import *
def main():
    spark = (
        SparkSession.builder
        .appName("Exercise7")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    ) # Use () to avoid \ while creating a session

    zip_path = "data/hard-drive-2022-01-01-failures.csv.zip"

    df = load_csv_from_zip(spark, zip_path)

    df = add_source_file_col(df, zip_path); 
    save_step(df, "step1_source_file")
    df = add_file_date_col(df); 
    save_step(df, "step2_file_date")
    df = add_brand_col(df); 
    save_step(df, "step3_brand")
    df = add_storage_ranking(df); 
    save_step(df, "step4_storage_ranking")
    df = add_primary_key(df); 
    save_step(df, "step5_primary_key")

    print("\nAll 5 CSV files written to the reports directory....\n")

if __name__ == "__main__":
    main()