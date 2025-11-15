import pandas as pd

# Load processed metadata
metadata_df = pd.read_csv("data/processed/metadata_processed.csv")

# Verified rows (having scheme_code)
verified_metadata = metadata_df.dropna(subset=['scheme_code'])
print("Number of verified funds for Metadata table:", len(verified_metadata))

# Load processed NAV
nav_df = pd.read_csv("data/processed/nav_processed.csv")

# Keep only NAVs with verified scheme_code
verified_nav = nav_df[nav_df['scheme_code'].isin(verified_metadata['scheme_code'])]
print("Number of NAV records for NAV history table:", len(verified_nav))
