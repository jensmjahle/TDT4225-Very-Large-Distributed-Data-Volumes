# ------------------------------------------------------------
# Task 8 Helper — Taxi Proximity Detection (≤5m & ≤5s)
# Optimized for large datasets with progress tracking & chunked output
# ------------------------------------------------------------
import os
import math
import pandas as pd
import time
import gc
from tabulate import tabulate
from helpers.haversine_helper import haversine


class Task8Helper:
    def __init__(self, cursor, db, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.db = db
        self.sql_folder = sql_folder

    # ------------------------------------------------------------
    # Run SQL safely (handles multi-statement scripts)
    # ------------------------------------------------------------
    def _run_sql(self, filename, fetch=True, silent=False):
        filepath = os.path.join(self.sql_folder, filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"SQL file not found: {filepath}")

        with open(filepath, "r", encoding="utf-8") as f:
            query = f.read().strip()

        if not silent:
            print(f"\nRunning {filename}...")

        # Run multi-statement queries safely
        for result in self.cursor.execute(query, multi=True):
            try:
                result.fetchall()
            except Exception:
                pass
            while self.cursor.nextset():
                pass

        if not fetch:
            return pd.DataFrame()

        # Re-run final SELECT if needed
        if query.lower().startswith("select"):
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            cols = self.cursor.column_names
            return pd.DataFrame(rows, columns=cols)

        return pd.DataFrame()

    # ------------------------------------------------------------
    # Fetch GPS points for a list of trips (chunked)
    # ------------------------------------------------------------
    def _get_trip_points(self, trip_ids):
        if not trip_ids:
            return pd.DataFrame()

        placeholders = ",".join(["%s"] * len(trip_ids))
        query = f"""
            SELECT trip_id, seq, latitude AS lat, longitude AS lon
            FROM Point
            WHERE trip_id IN ({placeholders});
        """
        self.cursor.execute(query, tuple(trip_ids))
        rows = self.cursor.fetchall()
        return pd.DataFrame(rows, columns=["trip_id", "seq", "lat", "lon"])

    # ------------------------------------------------------------
    # Check proximity (≤5m & ≤5s) — chunked & progress printed
    # ------------------------------------------------------------
    def _check_proximity(self, points_df, pair_df, chunk_size=2000, out_file="task8_proximity_pairs.csv"):
        print(f"\nStarting proximity check in chunks of {chunk_size} pairs...")
        total_pairs = len(pair_df)
        total_found = 0
        processed = 0

        # Prepare CSV file for incremental write
        if os.path.exists(out_file):
            os.remove(out_file)
        open(out_file, "w", encoding="utf-8").write(
            "taxi_a,taxi_b,trip_a,trip_b,distance_m,time_diff_s\n"
        )

        start_time = time.time()

        for start in range(0, total_pairs, chunk_size):
            end = min(start + chunk_size, total_pairs)
            batch = pair_df.iloc[start:end]
            results = []

            for _, row in batch.iterrows():
                a_id, b_id = row.trip_a, row.trip_b
                pts_a = points_df[points_df["trip_id"] == a_id].sort_values("seq")
                pts_b = points_df[points_df["trip_id"] == b_id].sort_values("seq")

                if pts_a.empty or pts_b.empty:
                    continue

                found = False
                for _, pa in pts_a.iterrows():
                    nearby = pts_b[
                        (pts_b["seq"] >= pa["seq"] - 1)
                        & (pts_b["seq"] <= pa["seq"] + 1)
                    ]
                    for _, pb in nearby.iterrows():
                        dist_km = haversine(pa["lat"], pa["lon"], pb["lat"], pb["lon"])
                        dist_m = dist_km * 1000
                        time_diff = abs(pa["seq"] - pb["seq"]) * 15
                        if dist_m <= 5 and time_diff <= 5:
                            results.append([
                                row.taxi_a, row.taxi_b, a_id, b_id,
                                round(dist_m, 2), time_diff
                            ])
                            found = True
                            break
                    if found:
                        break

            # Write results chunk to disk
            if results:
                pd.DataFrame(results).to_csv(
                    out_file, mode="a", header=False, index=False
                )
                total_found += len(results)

            processed += len(batch)
            elapsed = time.time() - start_time
            print(f"Processed {processed}/{total_pairs} pairs | "
                  f"Found {total_found} matches | Elapsed: {elapsed:.1f}s")

            # Free memory
            del batch, results
            gc.collect()

        print(f"\nCompleted proximity check. Total matches: {total_found}")
        print(f"Results written incrementally to: {out_file}")

    # ------------------------------------------------------------
    # Main runner
    # ------------------------------------------------------------
    def run_task8(self, chunk_size=2000):
        print("\n--- TASK 8: Taxi Proximity Detection (≤5m & ≤5s) ---")

        # 1️⃣ Create the staging table for trip start/end times
        print("Creating staging table trip_times_stage...")
        self._run_sql("create_temp_trip_times.sql", fetch=False)
        self.db.commit()
        print("trip_times_stage table ready.")

        # 2️⃣ Get all overlapping trip pairs
        print("Fetching overlapping trip pairs...")
        self.cursor.execute(open(os.path.join(self.sql_folder, "get_overlapping_trip_pairs.sql")).read())
        rows = self.cursor.fetchall()
        cols = self.cursor.column_names
        pair_df = pd.DataFrame(rows, columns=cols)

        if pair_df.empty:
            print("No overlapping pairs found.")
            return

        print(f"Found {len(pair_df):,} overlapping trip pairs to check.")

        # 3️⃣ Fetch GPS points for involved trips
        trip_ids = set(pair_df["trip_a"]) | set(pair_df["trip_b"])
        print(f"Fetching GPS points for {len(trip_ids):,} trips...")
        points_df = self._get_trip_points(trip_ids)
        print(f"Loaded {len(points_df):,} GPS points.")

        # 4️⃣ Run chunked proximity check & write incrementally to file
        self._check_proximity(points_df, pair_df, chunk_size=chunk_size)

        # 5️⃣ Print sample output
        out_file = "task8_proximity_pairs.csv"
        if os.path.exists(out_file):
            df = pd.read_csv(out_file)
            print("\nTop 20 results (preview):")
            print(tabulate(df.head(20), headers="keys", tablefmt="fancy_grid", showindex=False))

        print("\nTask 8 completed successfully.")
