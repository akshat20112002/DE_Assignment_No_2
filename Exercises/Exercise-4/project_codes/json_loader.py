import json
import pandas as pd

def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def flatten_json(data):
    if isinstance(data, dict):
        return pd.json_normalize([data])
    elif isinstance(data, list):
        return  pd.json_normalize(data)
    else:
        raise ValueError("Error flattening the json files...")