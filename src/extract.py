import os
import json
import pandas as pd
import requests
from src.db_utils import load_config
from difflib import get_close_matches

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def load_metadata_csv(cfg):
    csv_path = cfg["source"]["csv_path"]
    print(f"📄 Reading CSV: {csv_path}")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    return pd.read_csv(csv_path)

def fetch_scheme_list(cfg):
    url = cfg["source"]["mf_list_url"]
    print(f"📡 Fetching scheme list: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()  # returns list of dicts

def map_scheme_codes(metadata_df, api_df):
    metadata_df['scheme_code'] = None
    api_names = api_df['scheme_name'].tolist()
    
    for i, row in metadata_df.iterrows():
        name = row['scheme_name'].strip()
        exact_match = api_df[api_df['scheme_name'] == name]
        if not exact_match.empty:
            metadata_df.at[i, 'scheme_code'] = exact_match.iloc[0]['scheme_code']
            continue
        close_matches = get_close_matches(name, api_names, n=1, cutoff=0.8)
        if close_matches:
            matched_name = close_matches[0]
            matched_code = api_df[api_df['scheme_name'] == matched_name]['scheme_code'].values[0]
            metadata_df.at[i, 'scheme_code'] = matched_code

    mapped_count = metadata_df['scheme_code'].notnull().sum()
    missing_count = len(metadata_df) - mapped_count
    print(f"✅ Verified funds → {mapped_count}")
    print(f"⚠️ Unverified funds → {missing_count}")
    return metadata_df

def extract():
    cfg = load_config()
    raw_folder = os.path.join("data", "raw")
    ensure_dir(raw_folder)

    print("\n🚀 Starting Extraction Phase...\n")
    metadata_df = load_metadata_csv(cfg)

    mf_list = fetch_scheme_list(cfg)
    api_df = pd.DataFrame(mf_list)[['schemeCode', 'schemeName']]
    api_df.rename(columns={'schemeCode': 'scheme_code', 'schemeName': 'scheme_name'}, inplace=True)

    metadata_df = map_scheme_codes(metadata_df, api_df)

    # Save JSON
    metadata_path = os.path.join(raw_folder, "metadata_raw.json")
    metadata_df.to_json(metadata_path, orient="records", indent=4)
    print(f"✅ Saved raw metadata → {metadata_path}")

if __name__ == "__main__":
    extract()
