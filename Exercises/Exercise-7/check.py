import zipfile

with zipfile.ZipFile("data/hard-drive-2022-01-01-failures.csv.zip") as z:
    print(z.namelist())