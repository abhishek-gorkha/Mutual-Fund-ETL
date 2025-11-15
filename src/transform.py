import os
import pandas as pd
import json

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def transform():
    raw_folder = os.path.join("data", "raw")
    processed_folder = os.path.join("data", "processed")
    ensure_dir(processed_folder)

    # Load raw metadata
    metadata_path = os.path.join(raw_folder, "metadata_raw.json")
    print(f"📄 Loading raw metadata: {metadata_path}")
    with open(metadata_path, "r") as f:
        metadata = json.load(f)
    df = pd.DataFrame(metadata)

    # Split verified and unverified
    verified_df = df[df['scheme_code'].notnull()].copy()
    unverified_df = df[df['scheme_code'].isnull()].copy()

    # Save CSVs
    verified_path = os.path.join(processed_folder, "metadata_verified.csv")
    unverified_path = os.path.join(processed_folder, "metadata_unverified.csv")

    verified_df.to_csv(verified_path, index=False)
    unverified_df.to_csv(unverified_path, index=False)

    print(f"✅ Saved verified metadata → {verified_path} ({len(verified_df)} rows)")
    print(f"⚠️ Saved unverified metadata → {unverified_path} ({len(unverified_df)} rows)")
    print("\n🎉 Transform Completed!")

if __name__ == "__main__":
    transform()
