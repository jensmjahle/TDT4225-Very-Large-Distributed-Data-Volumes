
import os
import time
import pandas as pd
from tabulate import tabulate
from helpers.haversine_helper import haversine


class Task5Helper:
    def __init__(self, cursor, db, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.db = db
        self.sql_folder = sql_folder

    def _run_sql(self, filename):
        path = os.path.join(self.sql_folder, filename)
        print(f"Running SQL file: {filename}")
        with open(path, "r", encoding="utf-8") as f:
            query = f.read().strip()

        start = time.time()
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        cols = self.cursor.column_names
        print(f"Executed in {time.time() - start:.2f}s")
        return pd.DataFrame(rows, columns=cols)

    def run_task5(self, chunk_size=10000):
        print("\n--- TASK 5: Total Hours & Distance per Taxi ---")
        df = self._run_sql("task5_taxi_hours_distance.sql")

        if df.empty:
            print("No data to process.")
            return

        df["start_time"] = pd.to_datetime(df["start_time"])
        df["end_time"] = pd.to_datetime(df["end_time"])
        df[["start_lat", "start_lon", "end_lat", "end_lon"]] = df[
            ["start_lat", "start_lon", "end_lat", "end_lon"]
        ].astype(float)

        print(f"Loaded {len(df):,} trips. Computing metrics...\n")

        results = []
        taxis = df["taxi_id"].unique()
        total_taxis = len(taxis)
        start_time = time.time()

        for i, taxi_id in enumerate(taxis, start=1):
            tdf = df[df["taxi_id"] == taxi_id]
            total_hours = ((tdf["end_time"] - tdf["start_time"]).dt.total_seconds() / 3600).sum()

            tdf["distance_km"] = [
                haversine(row.start_lat, row.start_lon, row.end_lat, row.end_lon)
                for row in tdf.itertuples(index=False)
            ]
            total_distance = tdf["distance_km"].sum()

            results.append({
                "taxi_id": taxi_id,
                "total_hours": round(total_hours, 2),
                "total_distance_km": round(total_distance, 2)
            })

            if i % chunk_size == 0 or i == total_taxis:
                elapsed = time.time() - start_time
                print(f"Processed {i}/{total_taxis} taxis ({(i/total_taxis)*100:.1f}%) | {elapsed/60:.1f} min")

        result_df = pd.DataFrame(results).sort_values("total_hours", ascending=False)
        result_df.to_csv("task5_taxi_hours_distance.csv", index=False)

        print("\nTop 20 taxis (by total hours):")
        print(tabulate(result_df.head(20), headers="keys", tablefmt="fancy_grid", showindex=False))
        print(f"\nSaved results → task5_taxi_hours_distance.csv ({len(result_df)} rows)")
