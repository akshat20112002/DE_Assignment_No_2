from pathlib import Path

def writer_csv(df, json_file_path, output_folder):
    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)
    
        # Path(json_file_path).stem returns the filename without its extension.
    # Example: "data/sample.json" -> "sample" --> sample.csv
    csv_file_path = output_folder / f"{Path(json_file_path).stem}.csv"
    df.to_csv(csv_file_path, index=False)
    print(f"Converted: {json_file_path} to {csv_file_path}")