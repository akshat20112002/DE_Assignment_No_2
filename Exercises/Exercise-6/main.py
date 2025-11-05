import zipfile
import os
def main():
    """
    Extracts all CSV files from ZIP archives in a source directory
    to a specified destination directory.
    """
    source_directory = "data"
    destination_directory = "csv_files"
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)
    for filename in os.listdir(source_directory):
        if filename.endswith(".zip"):
            zip_filepath = os.path.join(source_directory, filename)
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                for file_in_zip in zip_ref.namelist():
                    if file_in_zip.startswith("__MACOSX/"):
                        continue

                    if file_in_zip.endswith(".csv"):
                        zip_ref.extract(file_in_zip, destination_directory)
                        print(f"Extracted '{file_in_zip}' from '{filename}' to '{destination_directory}'")
if __name__ == "__main__":
    main()
# from pyspark.sql import SparkSession


# def main():
#     spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
#     # your code here


# if __name__ == "__main__":
#     main()
