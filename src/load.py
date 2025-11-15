import json
import os
import psycopg2
from psycopg2.extras import execute_values
from yaml import safe_load

# Load config
CONFIG_PATH = "config/config.yaml"
with open(CONFIG_PATH) as f:
    config = safe_load(f)

DB_CONFIG = config['database']

# Paths
RAW_JSON_PATH = "data/Raw/metadata_raw.json"
PROCESSED_VERIFIED = "data/Processed/metadata_verified/metadata_verified.json"
PROCESSED_UNVERIFIED = "data/Processed/metadata_unverified/metadata_unverified.json"

# Table names
TABLE_VERIFIED = "mutual_fund_metadata"
TABLE_UNVERIFIED = "mutual_fund_unverified"

# 21 columns (adjust column names as per your actual DB)
COLUMNS = [
    "scheme_code", "scheme_name", "fund_type", "fund_category", "returns_1yr",
    "returns_3yr", "returns_5yr", "nav", "aum", "expense_ratio", "rating",
    "inception_date", "risk_level", "fund_manager", "benchmark", "min_investment",
    "exit_load", "dividend_frequency", "latest_nav_date", "direct_plan", "growth_plan"
]

def load_raw_data():
    if not os.path.exists(RAW_JSON_PATH):
        print(f"Raw file not found: {RAW_JSON_PATH}")
        return []
    
    with open(RAW_JSON_PATH, "r") as f:
        data = json.load(f)
    print(f"Loaded {len(data)} records from raw JSON.")
    return data

def split_verified_unverified(funds):
    verified = [f for f in funds if f.get("is_verified") is True]
    unverified = [f for f in funds if f.get("is_verified") is not True]
    print(f"Verified: {len(verified)}, Unverified: {len(unverified)}")
    return verified, unverified

def save_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} records to {path}")

def create_table_if_not_exists(table):
    columns_sql = ", ".join([f'"{col}" TEXT' for col in COLUMNS])
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        {columns_sql}
    )
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(create_sql)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Table `{table}` is ready.")
    except Exception as e:
        print(f"Error creating table {table}: {e}")

def load_to_postgres(table, records):
    if not records:
        print(f"No data to load into {table}")
        return
    
    # Ensure table exists
    create_table_if_not_exists(table)

    # Prepare data for insertion
    values = []
    for r in records:
        row = []
        for col in COLUMNS:
            row.append(r.get(col))
        values.append(row)

    placeholders = ", ".join([f'"{col}"' for col in COLUMNS])
    sql = f"INSERT INTO {table} ({placeholders}) VALUES %s"

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        execute_values(cur, sql, values)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Inserted {len(records)} records into {table} table")
    except Exception as e:
        print(f"Error inserting into {table}: {e}")

def main():
    funds = load_raw_data()
    if not funds:
        print("No raw data found. Exiting.")
        return

    verified, unverified = split_verified_unverified(funds)

    save_json(verified, PROCESSED_VERIFIED)
    save_json(unverified, PROCESSED_UNVERIFIED)

    load_to_postgres(TABLE_VERIFIED, verified)
    load_to_postgres(TABLE_UNVERIFIED, unverified)

if __name__ == "__main__":
    main()
