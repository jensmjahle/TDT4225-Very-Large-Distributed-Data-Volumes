
import os
import pandas as pd
from tabulate import tabulate


class Task11Helper:
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

        return pd.DataFrame(rows, columns=columns)


    def run_task11(self):
        print("\n--- TASK 11: AVERAGE IDLE TIME PER TAXI ---")

        df = self._run_sql_file("task11_avg_idle_time.sql")

        if df.empty:
            print("No results found.")
            return

        # Save full results
        output_file = "task11_avg_idle_time.csv"
        df.to_csv(output_file, index=False)
        print(f"Saved results to {output_file} ({len(df)} rows).")

        # Print top 20
        print("\nTop 20 Taxis with Highest Average Idle Time:")
        print(
            tabulate(
                df.head(20),
                headers="keys",
                tablefmt="fancy_grid",
                showindex=False,
            )
        )

        print("\nTask 11 completed successfully.")
