import json
import pandas as pd
from pathlib import Path

def load_swiggy_json(json_path: str, output_path: str):
    with open(json_path, 'r') as f:
        data = json.load(f)

    df = pd.json_normalize(data)
    df.to_csv(output_path, index=False)
    print(f"Swiggy data saved to {output_path}")

if __name__ == "__main__":
    load_swiggy_json("data/data.json", "data/swiggy_restaurants.csv")