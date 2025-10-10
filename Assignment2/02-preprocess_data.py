# ------------------------------------------------------------
# Data Cleaning & Preprocessing for the Porto Taxi Dataset
# ------------------------------------------------------------
import json

import pandas as pd
import ast

# ------------------------------------------------------------
# Step 1. Load raw dataset
# ------------------------------------------------------------
print("\n===== STEP 1: LOADING RAW DATA =====")
file_path = "porto.csv"
df = pd.read_csv(file_path)
print(f"Loaded dataset with shape: {df.shape}")

# ------------------------------------------------------------
# Step 2. Remove incomplete trips (MISSING_DATA=True)
# ------------------------------------------------------------
print("\n===== STEP 2: REMOVE INCOMPLETE TRIPS =====")
before_missing = len(df)
df = df[df["MISSING_DATA"] == False]
removed_missing = before_missing - len(df)
print(f"Removed {removed_missing} incomplete trips (MISSING_DATA=True).")
print(f"Remaining trips: {len(df)}")

# ------------------------------------------------------------
# Step 3. Remove fully duplicated rows (all columns identical)
# ------------------------------------------------------------
print("\n===== STEP 3: REMOVE FULL DUPLICATES =====")
before_dupes = len(df)
df = df.drop_duplicates(keep="first")
removed_dupes = before_dupes - len(df)
print(f"Removed {removed_dupes} fully duplicated rows.")
print(f"Remaining trips: {len(df)}")

# ------------------------------------------------------------
# Step 4. Remove invalid CALL_TYPE combinations
# ------------------------------------------------------------
print("\n===== STEP 4: VALIDATE CALL_TYPE RULES =====")
before_ct = len(df)

# Rule for A: must have ORIGIN_CALL (not null), ORIGIN_STAND should be NaN
mask_A = (df["CALL_TYPE"] == "A") & (df["ORIGIN_CALL"].notna())

# Rule for B: must have ORIGIN_STAND (not null), ORIGIN_CALL should be NaN
mask_B = (df["CALL_TYPE"] == "B") & (df["ORIGIN_STAND"].notna())

# Rule for C: both ORIGIN_CALL and ORIGIN_STAND must be NaN
mask_C = (df["CALL_TYPE"] == "C") & (df["ORIGIN_CALL"].isna()) & (df["ORIGIN_STAND"].isna())

# Keep only rows that match any of the valid rules
df = df[mask_A | mask_B | mask_C]

removed_ct = before_ct - len(df)
print(f"Removed {removed_ct} rows that violated CALL_TYPE rules.")
print(f"Remaining trips: {len(df)}")

# ------------------------------------------------------------
# Step 5. Parse POLYLINE safely
# Parse the POLYLINE string into a list of [lon, lat] pairs.
# ------------------------------------------------------------
print("\n===== STEP 5: PARSING POLYLINE =====")

def parse_polyline(polyline_str):

    try:
        coords = json.loads(polyline_str)
        if isinstance(coords, list):
            return coords
        return []
    except Exception:
        return []

polylines = []
n = len(df)
progress_intervals = 10
checkpoint = n // progress_intervals if n >= progress_intervals else 1

for i, polyline_str in enumerate(df["POLYLINE"]):
    polylines.append(parse_polyline(polyline_str))
    if (i + 1) % checkpoint == 0 or (i + 1) == n:
        progress = int(((i + 1) / n) * 100)
        print(f"Parsing POLYLINE: {progress}% done...")

df["POLYLINE"] = polylines
df["num_points"] = df["POLYLINE"].apply(len)

print("Parsed POLYLINE successfully.")
print(df["num_points"].describe())

# ------------------------------------------------------------
# Step 6. Remove invalid trips (<3 GPS points)
# ------------------------------------------------------------
print("\n===== STEP 6: REMOVE INVALID TRIPS (<3 GPS POINTS) =====")
before_invalid = len(df)
df = df[df["num_points"] >= 3]
removed_invalid = before_invalid - len(df)
print(f"Removed {removed_invalid} invalid trips (fewer than 3 GPS points).")
print(f"Remaining trips: {len(df)}")

# ------------------------------------------------------------
# Step 7. Remove duplicate TRIP_IDs (discard all occurrences)
# ------------------------------------------------------------
print("\n===== STEP 7: REMOVE DUPLICATE TRIP IDS =====")
duplicate_ids = df[df.duplicated(subset="TRIP_ID", keep=False)]["TRIP_ID"].unique()
num_duplicate_ids = len(duplicate_ids)

if num_duplicate_ids > 0:
    before_id_dupes = len(df)
    df = df[~df["TRIP_ID"].isin(duplicate_ids)]
    removed_id_dupes = before_id_dupes - len(df)
    print(f"Removed {removed_id_dupes} rows with duplicate TRIP_IDs ({num_duplicate_ids} duplicate IDs).")
else:
    print("No duplicate TRIP_IDs found.")

print(f"Remaining trips: {len(df)}")

# ------------------------------------------------------------
# Step 8. Convert TIMESTAMP to DATETIME (last, for performance)
# ------------------------------------------------------------
print("\n===== STEP 8: CONVERT TIMESTAMP TO DATETIME =====")
df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], unit="s")
print("Converted TIMESTAMP column to DATETIME.")

# ------------------------------------------------------------
# Step 9. Save preprocessed data
# ------------------------------------------------------------
print("\n===== STEP 9: SAVE PREPROCESSED DATA =====")
output_file = "porto_preprocessed.csv"
df.to_csv(output_file, index=False)
print(f"Saved preprocessed dataset to '{output_file}'")
print(f"Final shape: {df.shape}")

print("\n=== PREPROCESSING COMPLETED SUCCESSFULLY ===")
