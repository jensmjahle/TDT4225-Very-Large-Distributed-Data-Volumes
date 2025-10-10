# ------------------------------------------------------------
# Task 8 Helper — Taxi Proximity Detection (5m & 5s rule)
# Bulk-friendly, resumable, progress-tracked version
# ------------------------------------------------------------
import os
import pandas as pd
import time
import sys
from tabulate import tabulate
from helpers.haversine_helper import haversine


class Task8Helper:
    def __init__(self, cursor, db, sql_folder="sql_tasks", output_dir="task8_output"):
        self.cursor = cursor
        self.db = db
        self.sql_folder = sql_folder
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    # ------------------------------------------------------------
    # Run SQL from file (supports multi-statement scripts)
    # ------------------------------------------------------------
    def _run_sql(self, filename, fetch=True):
        filepath = os.path.join(self.sql_folder, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            query = f.read().strip()

        try:
            # ✅ Allow multiple SQL statements in one file
            for result in self.cursor.execute(query, multi=True):
                pass

            if fetch:
                try:
                    rows = self.cursor.fetchall()
                    cols = self.cursor.column_names
                    return pd.DataFrame(rows, columns=cols)
                except Exception:
                    # No results to fetch
                    return pd.DataFrame()

            return pd.DataFrame()
        except Exception as e:
            print(f"Error running {filename}: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------
    # Fetch GPS points for a set of trip IDs
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
    # Check proximity (≤5m & ≤5s) for a subset of pairs
    # ------------------------------------------------------------
    def _check_proximity_chunk(self, points_df, pair_df, start_index, end_index):
        results = []
        n_pairs = len(pair_df)

        for i, pair in enumerate(pair_df.itertuples(index=False), start=start_index):
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
                    dist_m = haversine(pa["lat"], pa["lon"], pb["lat"], pb["lon"]) * 1000
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

            # Progress feedback
            if i % 100 == 0 or i == end_index:
                sys.stdout.write(f"\rChecked {i}/{n_pairs} pairs...")
                sys.stdout.flush()
        print()
        return pd.DataFrame(results)

    # ------------------------------------------------------------
    # Main runner
    # ------------------------------------------------------------
    def run_task8(self, limit_pairs=50000, chunk_size=5000):
        print("\n--- TASK 8: Taxi Proximity Detection (5m & 5s) ---")

        # 1️⃣ Create temporary start/end cache
        print("Creating temporary trip_times table...")
        self._run_sql("create_temp_trip_times.sql", fetch=False)
        self.db.commit()

        # 2️⃣ Fetch overlapping trip pairs
        sql_path = os.path.join(self.sql_folder, "get_overlapping_trip_pairs.sql")
        with open(sql_path, "r", encoding="utf-8") as f:
            query = f.read().replace("LIMIT 5000", f"LIMIT {limit_pairs}")
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        cols = self.cursor.column_names
        pair_df = pd.DataFrame(rows, columns=cols)

        if pair_df.empty:
            print("No overlapping pairs found.")
            return

        print(f"Found {len(pair_df):,} overlapping trip pairs needing distance checks.")

        # 3️⃣ Fetch all GPS points
        trip_ids = set(pair_df["trip_a"]) | set(pair_df["trip_b"])
        print(f"Fetching GPS points for {len(trip_ids):,} trips...")
        points_df = self._get_trip_points(trip_ids)
        print(f"Loaded {len(points_df):,} points for proximity check.")

        # 4️⃣ Process in resumable chunks
        total_pairs = len(pair_df)
        all_files = []
        start_time = time.time()

        existing_chunks = {
            f for f in os.listdir(self.output_dir)
            if f.startswith("task8_chunk_") and f.endswith(".csv")
        }

        for start in range(0, total_pairs, chunk_size):
            end = min(start + chunk_size, total_pairs)
            chunk_file = f"task8_chunk_{start}_{end}.csv"
            chunk_path = os.path.join(self.output_dir, chunk_file)

            # ✅ Skip already processed chunks
            if chunk_file in existing_chunks:
                print(f"Skipping already processed chunk {chunk_file}")
                all_files.append(chunk_path)
                continue

            chunk = pair_df.iloc[start:end]
            print(f"\nProcessing pairs {start + 1}–{end} / {total_pairs}...")

            chunk_results = self._check_proximity_chunk(points_df, chunk, start + 1, end)
            if not chunk_results.empty:
                chunk_results.to_csv(chunk_path, index=False)
                all_files.append(chunk_path)
                print(f"Saved {len(chunk_results)} close encounters → {chunk_path}")
            else:
                print("No matches in this chunk.")

            elapsed = time.time() - start_time
            progress = (end / total_pairs) * 100
            print(f"Elapsed: {elapsed:.1f}s | Progress: {progress:.1f}%")

        # 5️⃣ Combine all chunk outputs
        combined_path = os.path.join(self.output_dir, "task8_all_close_pairs.csv")
        print("\nCombining all chunk results...")
        combined = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
        combined.to_csv(combined_path, index=False)
        print(f"Saved combined results → {combined_path}")

        # 6️⃣ Summarize
        grouped = (
            combined.groupby(["taxi_a", "taxi_b"])
            .size()
            .reset_index(name="close_events")
            .sort_values("close_events", ascending=False)
        )
        summary_path = os.path.join(self.output_dir, "task8_summary.csv")
        grouped.to_csv(summary_path, index=False)
        print(f"Saved summary → {summary_path}")

        print("\nTop 20 Taxi Pairs (Close Encounters):")
        print(tabulate(grouped.head(20), headers="keys", tablefmt="fancy_grid", showindex=False))

        # 7️⃣ Cleanup temporary table
        try:
            self.cursor.execute("DROP TEMPORARY TABLE IF EXISTS trip_times;")
            self.db.commit()
            print("Temporary table trip_times dropped.")
        except Exception:
            print("Warning: Could not drop temporary table trip_times.")

        print("\nTask 8 completed successfully.")
