
import pandas as pd
from tabulate import tabulate
from collections import defaultdict
from helpers.haversine_helper import haversine


class Task5Helper:
    def __init__(self, cursor, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.sql_folder = sql_folder

    def _run_sql(self, filename):
        import os
        filepath = os.path.join(self.sql_folder, filename)
        print(f"\n===== Running {filename} =====")

        with open(filepath, "r", encoding="utf-8") as f:
            query = f.read().strip()

        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            columns = self.cursor.column_names
            if not rows:
                print("No results returned.")
                return pd.DataFrame()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print(f"Error running {filename}: {e}")
            return pd.DataFrame()

    def _compute_taxi_stats(self, df):
        if df.empty:
            print("No data to process.")
            return pd.DataFrame()

        print("\n===== Computing Total Hours & Distance per Taxi =====")

        taxi_stats = defaultdict(lambda: {"distance_km": 0.0, "duration_hr": 0.0})
        prev_taxi, prev_trip, prev_point = None, None, None

        for _, row in df.iterrows():
            taxi_id, trip_id, seq, lat, lon = row
            if trip_id != prev_trip:
                prev_trip = trip_id
                prev_point = (lat, lon)
                prev_taxi = taxi_id
                continue

            dist = haversine(prev_point[0], prev_point[1], lat, lon)
            taxi_stats[taxi_id]["distance_km"] += dist
            # 15 seconds between points → 15 / 3600 = 0.0041667 hours
            taxi_stats[taxi_id]["duration_hr"] += 0.0041667
            prev_point = (lat, lon)

        # Convert to DataFrame
        result_df = pd.DataFrame([
            [taxi, round(v["duration_hr"], 2), round(v["distance_km"], 2)]
            for taxi, v in taxi_stats.items()
        ], columns=["taxi_id", "total_hours", "total_distance_km"])

        # Sort by total hours driven
        result_df = result_df.sort_values(by="total_hours", ascending=False).reset_index(drop=True)

        print(tabulate(result_df.head(20), headers="keys", tablefmt="fancy_grid", showindex=False))
        print("Computed total driving time and distance for all taxis.")
        return result_df

    def run_task5(self):
        df = self._run_sql("task5_taxi_hours_distance.sql")
        stats_df = self._compute_taxi_stats(df)
        print("\nTask 5 completed successfully.")
