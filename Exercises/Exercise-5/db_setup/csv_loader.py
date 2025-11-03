import csv
import os
from db_setup.logs_config import get_logger

logger = get_logger(__name__)

def insert_csv(cursor, table_name, csv_file_path):
    """
    Insert data from the CSV file from it's corresponding SQL file
    """
    if not os.path.exists(csv_file_path):
        logger.warning(f"CSV File not found for the table: {table_name}")
        return
    
    try:
        with open(csv_file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
            columns = ",".join(headers)
            placeholders = ",".join(["%s"] * len(headers)) #Dynamically use for adding data according to the dataset or user
            insert_query = f"insert into {table_name} ({columns}) values ({placeholders})" 

            for row in reader:
                cursor.execute(insert_query, row)
        
        logger.info(f"Data inserted successfully into {table_name}.")
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise