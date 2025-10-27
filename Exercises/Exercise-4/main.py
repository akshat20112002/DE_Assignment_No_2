from project_codes.file_finder import json_find_files
from project_codes.json_loader import load_json, flatten_json
from project_codes.csv_writer import writer_csv
from pathlib import Path

def main():
    base_path = r"/home/developer/Data Engineering Assignment 2/data-engineering-practice/Exercises/Exercise-4/data"
    csv_path = r"/home/developer/Data Engineering Assignment 2/data-engineering-practice/Exercises/Exercise-4/csv_files"
    
    # Ensure the CSV output folder exists
    Path(csv_path).mkdir(parents=True, exist_ok=True)

    json_files = json_find_files(base_path)

    if not json_files:
        print(f"JSON files are not present...")
        return

    print(f"Found {len(json_files)} JSON file(s)\n\nConverting them to CSV...\n")

    for file in json_files:
        try:
            data = load_json(file)
            df = flatten_json(data)
            writer_csv(df, file, csv_path)

            # Print flattened data
            print(f"\nFlattened Data for {file.name}:\n")
            print(df)
            print("\n" + "="*150 + "\n")

        except Exception as e:
            print(f"Failed to process {file}: {e}")

if __name__ == "__main__":
    main()