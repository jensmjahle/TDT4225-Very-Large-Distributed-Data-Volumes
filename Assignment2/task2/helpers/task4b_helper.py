
import os
import pandas as pd
from collections import defaultdict
from tabulate import tabulate
from helpers.haversine_helper import haversine


class Task4BHelper:
    def __init__(self, cursor, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.sql_folder = sql_folder

    # ------------------------------------------------------------
    # Run SQL from file and return DataFrame
    # ------------------------------------------------------------
    def _run_sql_file(self, filename, silent=True):
        filepath = os.path.join(self.sql_folder, filename)

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                query = f.read().strip()

            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            columns = self.cursor.column_names
        except Exception as e:
            if not silent:
                print(f"Error running {filename}: {e}")
            return pd.DataFrame(), []

        if not rows or len(rows) == 0:
            if not silent:
                print(f"No results returned for {filename}.")
            return pd.DataFrame(), []

        df = pd.DataFrame(rows, columns=columns)
        return df, rows

    # ------------------------------------------------------------
    # Compute average distance per call_type using Haversine
    # ------------------------------------------------------------
    def _compute_avg_distance(self, rows, silent=True):
        if rows is None or len(rows) == 0:
            if not silent:
                print("⚠️ No data available to compute distances.")
            return pd.DataFrame(columns=["call_type", "avg_distance_km"])

        distances = defaultdict(list)
        prev_trip, prev_point, prev_call = None, None, None

        for row in rows:
            call_type, trip_id, seq, lat, lon = row
            trip_id = str(trip_id)

            if trip_id != prev_trip:
                prev_trip, prev_point, prev_call = trip_id, (lat, lon), call_type
                continue

            dist = haversine(prev_point[0], prev_point[1], lat, lon)
            distances[prev_call].append(dist)
            prev_point = (lat, lon)

        if not distances:
            return pd.DataFrame(columns=["call_type", "avg_distance_km"])

        result = pd.DataFrame(
            [(ct, round(sum(v) / len(v), 3)) for ct, v in distances.items()],
            columns=["call_type", "avg_distance_km"],
        )

        return result

    # ------------------------------------------------------------
    # Main runner for Task 4b
    # ------------------------------------------------------------
    def run_task4b(self):
        # Step 1: Average trip duration
        dur_df, _ = self._run_sql_file("task4b1_avg_duration.sql", silent=True)

        # Step 2: Time band shares
        time_df, _ = self._run_sql_file("task4b2_time_bands.sql", silent=True)

        # Step 3: Distance computation (Python part)
        _, dist_rows = self._run_sql_file("task4b3_distance_query.sql", silent=True)
        dist_df = self._compute_avg_distance(dist_rows, silent=True)

        # ------------------------------------------------------------
        # Combine all into one summary table
        # ------------------------------------------------------------
        merged = pd.DataFrame({"call_type": ["A", "B", "C"]})
        for df in [dur_df, dist_df, time_df]:
            if not df.empty:
                merged = pd.merge(merged, df, on="call_type", how="left")

        print("\n===== Combined Summary Table for Task 4b =====")
        print(tabulate(merged.fillna("N/A"), headers="keys", tablefmt="fancy_grid", showindex=False))

        print("Task 4b completed successfully.\n")
