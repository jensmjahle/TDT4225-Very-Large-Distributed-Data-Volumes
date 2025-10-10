
import os
import pandas as pd
from tabulate import tabulate


class Task10Helper:
    def __init__(self, cursor, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.sql_folder = sql_folder


    def _run_sql_file(self, filename):
        filepath = os.path.join(self.sql_folder, filename)
        print(f"\n===== Running {filename} =====")

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                query = f.read().strip()
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            columns = self.cursor.column_names
        except Exception as e:
            print(f"Error running {filename}: {e}")
            return pd.DataFrame()

        if not rows:
            print("No results returned.")
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=columns)
        return df


    def run_task10(self):
        print("\n--- TASK 10: CIRCULAR TRIPS ---")

        df = self._run_sql_file("task10_circular_trips.sql")

        if df.empty:
            print("No circular trips found.")
            return

        output_file = "task10_circular_trips.csv"
        df.to_csv(output_file, index=False)
        print(f"Saved results to {output_file} ({len(df)} rows).")

        print("\nFirst 20 Circular Trips:")
        print(tabulate(df.head(20), headers="keys", tablefmt="fancy_grid", showindex=False))

        print("\nTask 10 completed successfully.")
