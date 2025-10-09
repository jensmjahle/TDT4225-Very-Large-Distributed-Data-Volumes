# ------------------------------------------------------------
# Prepare cleaned Porto dataset for database insertion (streaming)
# Splits porto_preprocessed.csv into trips_clean.csv and points_clean.csv
# ------------------------------------------------------------

import pandas as pd
import json
import csv

# ------------------------------------------------------------
# Step 1. Load cleaned dataset
# ------------------------------------------------------------
print("\n===== STEP 1: LOADING CLEANED DATA =====")
file_path = "porto_preprocessed.csv"
df = pd.read_csv(file_path)
print(f"Loaded cleaned dataset with shape: {df.shape}")

# ------------------------------------------------------------
# Step 2. Prepare Trip table (metadata only)
# ------------------------------------------------------------
print("\n===== STEP 2: PREPARE TRIP TABLE =====")

trip_df = df[[
    "TRIP_ID",
    "TAXI_ID",
    "CALL_TYPE",
    "ORIGIN_CALL",
    "ORIGIN_STAND",
    "TIMESTAMP",
    "DAY_TYPE"
]].copy()

trip_df.columns = [
    "trip_id",
    "taxi_id",
    "call_type",
    "origin_call",
    "origin_stand",
    "timestamp",
    "day_type"
]

trip_df.to_csv("trips_clean.csv", index=False)
print(f"✅ Saved Trip table: trips_clean.csv ({trip_df.shape[0]} rows)")

# ------------------------------------------------------------
# Step 3. Prepare Point table (stream to disk)
# ------------------------------------------------------------
print("\n===== STEP 3: PREPARE POINT TABLE (STREAMING MODE) =====")

output_file = "points_clean.csv"

# Open CSV writer once
with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["trip_id", "seq", "latitude", "longitude"])  # header

    n = len(df)
    progress_intervals = 10
    checkpoint = n // progress_intervals if n >= progress_intervals else 1

    for i, row in df.iterrows():
        trip_id = row["TRIP_ID"]
        polyline = row["POLYLINE"]

        # Parse POLYLINE JSON string
        if isinstance(polyline, str):
            try:
                polyline = json.loads(polyline)
            except json.JSONDecodeError:
                polyline = []

        # Write directly to file instead of storing in memory
        for seq, (lon, lat) in enumerate(polyline):
            writer.writerow([trip_id, seq, lat, lon])

        if (i + 1) % checkpoint == 0 or (i + 1) == n:
            progress = int(((i + 1) / n) * 100)
            print(f"Processing points: {progress}% done...")

print(f"Finished streaming all points to {output_file}")
print("\n=== DATABASE PREPARATION COMPLETED SUCCESSFULLY ===")
