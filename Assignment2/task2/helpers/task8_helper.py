# ------------------------------------------------------------
# Task 8 Helper — Taxi Proximity Detection (5m & 5s rule)
# ------------------------------------------------------------
import os
import math
import pandas as pd
import time
import sys
from tabulate import tabulate
from helpers.haversine_helper import haversine


class Task8Helper:
    def __init__(self, cursor, db, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.db = db
        self.sql_folder = sql_folder

    # ------------------------------------------------------------
    # Run SQL from file
    # ------------------------------------------------------------
    def _run_sql(self, filename, fetch=True):
        filepath = os.path.join(self.sql_folder, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            query = f.read().strip()

        self.cursor.execute(query)
        if fetch:
            rows = self.cursor.fetchall()
            cols = self.cursor.column_names
            return pd.DataFrame(rows, columns=cols)
        return pd.DataFrame()

    # ------------------------------------------------------------
    # Fetch GPS points for a list of trips
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
    # Check proximity (≤5m & ≤5s) with progress display
    # ------------------------------------------------------------
    def _check_proximity(self, points_df, pair_df):
        results = []
        n_pairs = len(pair_df)
        start_time = time.time()

        for i, pair in enumerate(pair_df.itertuples(index=False), start=1):
            a_id, b_id = pair.trip_a, pair.trip_b

            pts_a = points_df[points_df["trip_id"] == a_id].sort_values("seq")
            pts_b = points_df[points_df["trip_id"] == b_id].sort_values("seq")

            if pts_a.empty or pts_b.empty:
                continue

            found = False
            for _, pa in pts_a.iterrows():
                nearby = pts_b[
                    (pts_b["seq"] >= pa["seq"] - 1) & (pts_b["seq"] <= pa["seq"] + 1)
                ]
                for _, pb in nearby.iterrows():
                    dist_km = haversine(pa["lat"], pa["lon"], pb["lat"], pb["lon"])
                    dist_m = dist_km * 1000
                    time_diff = abs(pa["seq"] - pb["seq"]) * 15
                    if dist_m <= 5 and time_diff <= 5:
                        results.append({
                            "taxi_a": pair.taxi_a,
                            "taxi_b": pair.taxi_b,
                            "trip_a": a_id,
                            "trip_b": b_id,
                            "distance_m": round(dist_m, 2),
                            "time_diff_s": time_diff,
                        })
                        found = True
                        break
                if found:
                    break

            # Progress output
            if i % 100 == 0 or i == n_pairs:
                percent = (i / n_pairs) * 100
                elapsed = time.time() - start_time
                sys.stdout.write(
                    f"\r🔍 Checked {i}/{n_pairs} pairs ({percent:.1f}%) | "
                    f"Found {len(results)} close events | {elapsed:.1f}s elapsed"
                )
                sys.stdout.flush()

        print()  # newline
        return pd.DataFrame(results)

    # ------------------------------------------------------------
    # Main runner
    # ------------------------------------------------------------
    def run_task8(self, limit_pairs=5000):
        print("\n--- TASK 8: Taxi Proximity Detection (5m & 5s) ---")

        # 1️⃣ Create temporary table with cached start/end times
        print("⏳ Creating temporary trip_times table...")
        self._run_sql("create_temp_trip_times.sql", fetch=False)
        self.db.commit()
        print("✅ Temporary table ready.")

        # 2️⃣ Fetch overlapping trip pairs
        print("🔍 Fetching overlapping trip pairs (SQL pre-filter)...")
        sql_path = os.path.join(self.sql_folder, "get_overlapping_trip_pairs.sql")
        with open(sql_path, "r", encoding="utf-8") as f:
            query = f.read().replace("LIMIT 5000", f"LIMIT {limit_pairs}")
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        cols = self.cursor.column_names
        pair_df = pd.DataFrame(rows, columns=cols)

        if pair_df.empty:
            print("⚠️ No overlapping pairs found.")
            return

        print(f"✅ Found {len(pair_df):,} overlapping trip pairs needing distance checks.")

        # 3️⃣ Fetch all GPS points
        trip_ids = set(pair_df["trip_a"]) | set(pair_df["trip_b"])
        print(f"📡 Fetching GPS points for {len(trip_ids):,} trips...")
        points_df = self._get_trip_points(trip_ids)
        print(f"✅ Loaded {len(points_df):,} points for proximity check.")

        # 4️⃣ Run the proximity computation with progress bar
        print("🚀 Running distance checks (this may take a while)...")
        close_df = self._check_proximity(points_df, pair_df)

        if close_df.empty:
            print("⚠️ No close encounters (≤5m & ≤5s) found.")
            return

        # 5️⃣ Summarize, save & print top 20
        grouped = (
            close_df.groupby(["taxi_a", "taxi_b"])
            .size()
            .reset_index(name="close_events")
            .sort_values("close_events", ascending=False)
        )

        print("\n===== Top 20 Taxi Pairs (Close Encounters) =====")
        print(tabulate(grouped.head(20), headers="keys", tablefmt="fancy_grid", showindex=False))

        grouped.to_csv("task8_proximity_pairs.csv", index=False)
        print(f"\n💾 Saved results to task8_proximity_pairs.csv ({len(grouped)} total pairs).")

        print("\n✅ Task 8 completed successfully.")
