from pathlib import Path
def json_find_files(base_path):
    path = Path(base_path)
    return list(path.rglob('*.json'))