# import os
# import requests
# import zipfile
# from pathlib import Path

# download_uris = [
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
#     "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",  # Invalid link
# ]

# DOWNLOAD_DIR = Path("downloads")


# def ensure_download_dir():
#     """Create the 'downloads' folder if it doesn't exist."""
#     if not DOWNLOAD_DIR.exists():
#         DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
#         print(f"Created directory: {DOWNLOAD_DIR}")
#     else:
#         print(f"Directory already exists: {DOWNLOAD_DIR}")


# def download_file(url):
#     """Download a single file from the given URL."""
#     filename = url.split("/")[-1]
#     file_path = DOWNLOAD_DIR / filename

#     try:
#         print(f"Downloading {filename} ...")
#         response = requests.get(url, stream=True, timeout=15)
#         response.raise_for_status()

#         with open(file_path, "wb") as f:
#             for chunk in response.iter_content(chunk_size=1024):
#                 if chunk:
#                     f.write(chunk)
#         print(f"Downloaded: {filename}")
#         return file_path

#     except requests.exceptions.RequestException as e:
#         print(f"Failed to download {filename}: {e}")
#         return None


# def unzip_file(zip_path):
#     """Unzip a downloaded ZIP file and delete it afterward."""
#     if not zip_path or not zip_path.exists():
#         return

#     try:
#         with zipfile.ZipFile(zip_path, "r") as zip_ref:
#             zip_ref.extractall(DOWNLOAD_DIR)
#         print(f"Extracted: {zip_path.name}")
#     except zipfile.BadZipFile:
#         print(f"Bad zip file: {zip_path.name}")
#     finally:
#         zip_path.unlink(missing_ok=True)
#         print(f"Deleted zip: {zip_path.name}")


# def main():
#     ensure_download_dir()  # Creates folder if missing

#     for uri in download_uris:
#         zip_path = download_file(uri)
#         if zip_path:
#             unzip_file(zip_path)


# if __name__ == "__main__":
#     main()

import asyncio
import aiohttp
import aiofiles
import zipfile
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


def ensure_download_dir():
    """Create the 'downloads' folder if it doesn't exist."""
    if not DOWNLOAD_DIR.exists():
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {DOWNLOAD_DIR}")
    else:
        print(f"Directory already exists: {DOWNLOAD_DIR}")


async def download_file(session, url):
    """Download a single file asynchronously."""
    filename = url.split("/")[-1]
    file_path = DOWNLOAD_DIR / filename

    try:
        print(f"Downloading {filename} ...")
        async with session.get(url, timeout=30) as response:
            response.raise_for_status()
            async with aiofiles.open(file_path, "wb") as f:
                async for chunk in response.content.iter_chunked(1024):
                    await f.write(chunk)
        print(f"Downloaded: {filename}")
        return file_path
    except Exception as e:
        print(f"Failed to download {filename}: {e}")
        return None


def unzip_file(zip_path):
    """Unzip a downloaded ZIP file and delete it afterward."""
    if not zip_path or not zip_path.exists():
        return
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        print(f"Extracted: {zip_path.name}")
    except zipfile.BadZipFile:
        print(f"Bad zip file: {zip_path.name}")
    finally:
        zip_path.unlink(missing_ok=True)
        print(f"Deleted zip: {zip_path.name}")


async def download_and_unzip(session, url):
    """Download and unzip a file asynchronously."""
    zip_path = await download_file(session, url)
    if zip_path:
        unzip_file(zip_path)


async def main():
    ensure_download_dir()

    async with aiohttp.ClientSession() as session:
        tasks = [download_and_unzip(session, url) for url in download_uris]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
