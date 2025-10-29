# ------------------------------------------------------------
# Prepare cleaned Movies dataset for MongoDB insertion
# ------------------------------------------------------------
import pandas as pd
import os
import json

print("\n===== STEP 1: LOADING CLEANED DATA =====")

DATA_DIR = "../movies_clean"

files = {
    "movies": os.path.join(DATA_DIR, "movies_clean.csv"),
    "credits": os.path.join(DATA_DIR, "credits_clean.csv"),
    "keywords": os.path.join(DATA_DIR, "keywords_clean.csv"),
    "links": os.path.join(DATA_DIR, "links_clean.csv"),
    "ratings": os.path.join(DATA_DIR, "ratings_clean.csv")
}

dfs = {name: pd.read_csv(path) for name, path in files.items()}
for name, df in dfs.items():
    print(f"{name}: {df.shape}")

print("\n===== STEP 2: SAMPLE PREVIEW =====")
for name, df in dfs.items():
    print(f"\n{name.upper()} sample:")
    print(df.head(2))

print("\n=== DATA READY FOR MONGODB INSERTION ===")
