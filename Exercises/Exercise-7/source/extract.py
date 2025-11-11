import zipfile
import tempfile
def load_csv_from_zip(spark, zip_path):
    tmp_dir = tempfile.mkdtemp()

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(tmp_dir)
        csv_names = [f for f in z.namelist() if f.endswith(".csv")]

    print("EXTRACTED TO:", tmp_dir)

    return spark.read.option("header", True).csv(f"{tmp_dir}/{csv_names[0]}")