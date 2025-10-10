# ------------------------------------------------------------
# Task 6 Helper - Trips passing within 100 m of Porto City Hall
# ------------------------------------------------------------
import os
import pandas as pd
from tabulate import tabulate
from helpers.haversine_helper import haversine

class Task6Helper:
    def __init__(self, cursor, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.sql_folder = sql_folder

    def _run_sql(self, filename):
        filepath = os.path.join(self.sql_folder, filename)
        print(f"\n===== Running {filename} =====")

        with open(filepath, "r", encoding="utf-8") as f:
            query = f.read().strip()
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        columns = self.cursor.column_names
        if not rows:
            print("No results returned from SQL query.")
            return pd.DataFrame(columns=["trip_id", "latitude", "longitude"])
        return pd.DataFrame(rows, columns=columns)

    def _filter_within_100m(self, df):
        print("\n===== Filtering points within 100 m of City Hall =====")

        city_lat, city_lon = 41.15794, -8.62911
        radius_km = 0.1  # 100 m

        if df.empty:
            print("No candidate points found.")
            return pd.DataFrame(columns=["trip_id"])

        nearby_trips = set()
        for _, row in df.iterrows():
            dist = haversine(city_lat, city_lon, row["latitude"], row["longitude"])
            if dist <= radius_km:
                nearby_trips.add(row["trip_id"])

        result = pd.DataFrame(sorted(list(nearby_trips)), columns=["trip_id"])
        print(f"Found {len(result)} trips within 100 m of City Hall.")
        print(tabulate(result.head(20), headers='keys', tablefmt='fancy_grid', showindex=False))
        return result

    def run_task6(self):
        print("\n--- TASK 6 ---")
        df = self._run_sql("task6_near_cityhall.sql")
        trips_df = self._filter_within_100m(df)
        trips_df.to_csv("task6_near_cityhall.csv", index=False)
        print("Saved trips to task6_near_cityhall.csv")
        print("\nTask 6 completed successfully.")
