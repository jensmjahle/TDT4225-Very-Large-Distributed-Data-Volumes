
import os
import time
import pandas as pd
from tabulate import tabulate
from helpers.haversine_helper import haversine


class Task4BHelper:
    def __init__(self, cursor, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.sql_folder = sql_folder

    def _run_sql_file(self, filename, silent=False):
        path = os.path.join(self.sql_folder, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing SQL file: {path}")

        with open(path, "r", encoding="utf-8") as f:
            query = f.read().strip()

        if not silent:
            print(f"\n===== Running {filename} =====")

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if not rows:
            print("No results returned.")
            return pd.DataFrame()

        cols = self.cursor.column_names
        df = pd.DataFrame(rows, columns=cols)
        print(tabulate(df, headers="keys", tablefmt="fancy_grid", showindex=False))
        return df

    def _compute_avg_distance(self):
        print("\n===== Computing Average Trip Distance (Streaming) =====")
        qpath = os.path.join(self.sql_folder, "task4b3_distance_query.sql")
        with open(qpath, "r", encoding="utf-8") as f:
            query = f.read().strip()

        self.cursor.execute(query)

        prev_trip, prev_point, prev_call = None, None, None
        trip_distances = {}
        processed = 0
        last_log = time.time()

        for (call_type, trip_id, seq, lat, lon) in self.cursor:
            processed += 1
            trip_id = str(trip_id)

            if trip_id != prev_trip:
                prev_trip, prev_point, prev_call = trip_id, (lat, lon), call_type
                if trip_id not in trip_distances:
                    trip_distances[trip_id] = {"call_type": call_type, "dist": 0.0}
                continue

            dist = haversine(prev_point[0], prev_point[1], lat, lon)
            trip_distances[trip_id]["dist"] += dist
            prev_point = (lat, lon)

            if processed % 500000 == 0 or (time.time() - last_log) > 10:
                print(f"Processed {processed:,} GPS points...")
                last_log = time.time()

        print(f"Finished processing {processed:,} points.")

        # Aggregate per call_type
        agg = {}
        for _, info in trip_distances.items():
            agg.setdefault(info["call_type"], []).append(info["dist"])

        if not agg:
            print("No distances computed.")
            return pd.DataFrame(columns=["call_type", "avg_distance_km"])

        df = pd.DataFrame(
            [(ct, round(sum(v) / len(v), 3)) for ct, v in agg.items()],
            columns=["call_type", "avg_distance_km"]
        )
        print(tabulate(df, headers="keys", tablefmt="fancy_grid", showindex=False))
        return df


    def run_task4b(self):
        print("\n--- TASK 4b ---")

        dur_df = self._run_sql_file("task4b1_avg_duration.sql")
        time_df = self._run_sql_file("task4b2_time_bands.sql")
        dist_df = self._compute_avg_distance()

        print("\n===== Combined Summary Table for Task 4b =====")

        merged = pd.DataFrame({"call_type": ["A", "B", "C"]})
        for df in [dur_df, dist_df, time_df]:
            if not df.empty:
                merged = pd.merge(merged, df, on="call_type", how="left")

        print(tabulate(merged.fillna("N/A"), headers="keys", tablefmt="fancy_grid", showindex=False))
        merged.to_csv("task4b_summary.csv", index=False)
        print("Saved summary to task4b_summary.csv")

        print("\nTask 4b completed successfully.")
