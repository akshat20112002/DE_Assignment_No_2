import requests
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
from zipfile import ZipFile
import random

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
DOWNLOAD_DIR = Path("downloads")
TARGET_TIMESTAMP = "2024-01-19 15:45"


def setup_dirs():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


def get_files_by_timestamp(url, timestamp):
    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    return [
        row.find("a")["href"]
        for row in soup.find_all("tr")
        if len(row.find_all("td")) > 1
        and row.find_all("td")[1].get_text(strip=True) == timestamp
    ]


def analyze_csv(file_path):
    df = pd.read_csv(file_path, low_memory=False)
    if "HourlyDryBulbTemperature" not in df.columns or "DATE" not in df.columns:
        print(f"{file_path.name}: Missing required columns")
        return

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["HourlyDryBulbTemperature"] = pd.to_numeric(
        df["HourlyDryBulbTemperature"], errors="coerce"
    )
    max_temp = df["HourlyDryBulbTemperature"].max()
    max_records = df[df["HourlyDryBulbTemperature"] == max_temp][
        ["DATE", "HourlyDryBulbTemperature"]
    ]

    print(f"\nAnalysis for {file_path.name}:")
    for _, row in max_records.iterrows():
        date_time = row["DATE"]
        print(
            f"Date: {date_time.date()}, Time: {date_time.time()}, Temp: {row['HourlyDryBulbTemperature']}"
        )


def process_file(filename):
    local_path = DOWNLOAD_DIR / filename
    if not local_path.exists():
        print(f"Downloading {filename}...")
        with open(local_path, "wb") as f:
            f.write(requests.get(BASE_URL + filename).content)

    csv_files = []
    if filename.endswith(".zip"):
        with ZipFile(local_path) as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
            csv_files.extend(
                DOWNLOAD_DIR / f for f in zip_ref.namelist() if f.endswith(".csv")
            )
    elif filename.endswith(".csv"):
        csv_files.append(local_path)

    for csv_file in csv_files:
        analyze_csv(csv_file)


def main():
    setup_dirs()
    files = get_files_by_timestamp(BASE_URL, TARGET_TIMESTAMP)

    if not files:
        print(f"No files found with timestamp {TARGET_TIMESTAMP}")
        return

    print(f"Found {len(files)} files with timestamp {TARGET_TIMESTAMP}")
    chosen_file = random.choice(files)
    print(f"Selected: {chosen_file}")
    process_file(chosen_file)
    print(f"Analysis completed for {chosen_file}")


if __name__ == "__main__":
    main()
