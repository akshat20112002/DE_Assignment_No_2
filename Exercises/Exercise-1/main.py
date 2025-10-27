import requests
from concurrent.futures import ThreadPoolExecutor
import zipfile
from io import BytesIO
from pathlib import Path

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",  # Invalid link
]

DOWNLOAD_DIR = Path("downloads")


def ensure_dir():
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    print(f"Using directory: {DOWNLOAD_DIR}")


def download_csv(url):
    filename = Path(url).name.replace(".zip", ".csv")
    csv_path = DOWNLOAD_DIR / filename
    if csv_path.exists():
        print(f"Already exists: {filename}")
        return
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        with zipfile.ZipFile(BytesIO(resp.content)) as z:
            csv_in_zip = z.namelist()[0]  # Assumes single CSV per zip
            csv_content = z.read(csv_in_zip)
        with open(csv_path, "wb") as f:
            f.write(csv_content)
        print(f"Downloaded CSV: {filename}")
    except Exception as e:
        print(f"Failed {filename}: {e}")


def main():
    ensure_dir()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(download_csv, url) for url in download_uris]
        for future in futures:
            future.result()  # Blocks until all complete


if __name__ == "__main__":
    main()
