import requests
from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
DOWNLOAD_DIR = Path("downloads")
TARGET_TIMESTAMP = "2024-01-19 14:51"
MAX_WORKERS = 10  # Number of concurrent downloads


def ensure_download_dir():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloads folder ready at: {DOWNLOAD_DIR}")


def get_html(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text


def find_files_by_timestamp(html, timestamp):
    soup = BeautifulSoup(html, "html.parser")
    files = []
    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) < 2:
            continue
        last_modified = cols[1].get_text(strip=True)
        link = row.find("a")
        if link and last_modified == timestamp:
            filename = link.get("href")
            files.append(filename)
    return files


def analyze_csv(file_path):
    """
    Print only the timestamp and highest HourlyDryBulbTemperature.
    """
    df = pd.read_csv(file_path, low_memory=False)

    if "HourlyDryBulbTemperature" not in df.columns or "DATE" not in df.columns:
        return

    df["HourlyDryBulbTemperature"] = pd.to_numeric(
        df["HourlyDryBulbTemperature"], errors="coerce"
    )
    max_temp = df["HourlyDryBulbTemperature"].max(skipna=True)
    max_records = df[df["HourlyDryBulbTemperature"] == max_temp]

    # Print only DATE (timestamp) and HourlyDryBulbTemperature
    print(max_records[["DATE", "HourlyDryBulbTemperature"]].to_string(index=False))


def download_and_process(filename):
    url = BASE_URL + filename
    local_path = DOWNLOAD_DIR / filename
    if not local_path.exists():
        print(f"Downloading {filename} ...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded {filename}")

    csv_files = []
    if filename.endswith(".zip"):
        with ZipFile(local_path, "r") as zip_ref:
            extracted = zip_ref.namelist()
            zip_ref.extractall(DOWNLOAD_DIR)
            csv_files.extend(
                [DOWNLOAD_DIR / f for f in extracted if f.endswith(".csv")]
            )
    elif filename.endswith(".csv"):
        csv_files.append(local_path)

    for csv_file in csv_files:
        analyze_csv(csv_file)


def main():
    ensure_download_dir()
    html = get_html(BASE_URL)
    files = find_files_by_timestamp(html, TARGET_TIMESTAMP)

    if not files:
        print("No files found with the specified timestamp.")
        return

    print(f"Found {len(files)} file(s) with timestamp {TARGET_TIMESTAMP}: {files}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_and_process, f) for f in files]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing a file: {e}")


if __name__ == "__main__":
    main()
