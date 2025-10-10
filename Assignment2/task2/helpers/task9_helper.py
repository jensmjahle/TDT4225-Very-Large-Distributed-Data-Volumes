# ------------------------------------------------------------
# Task 9 Helper: Midnight Crossers
# Finds trips that start one day and end the next.
# ------------------------------------------------------------
import os
import pandas as pd
from tabulate import tabulate


class Task9Helper:
    def __init__(self, cursor, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.sql_folder = sql_folder

    # ------------------------------------------------------------
    # Run SQL and return DataFrame
    # ------------------------------------------------------------
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

    # ------------------------------------------------------------
    # Main runner for Task 9
    # ------------------------------------------------------------
    def run_task9(self):
        print("\n--- TASK 9: MIDNIGHT CROSSERS ---")

        df = self._run_sql_file("task9_midnight_crossers.sql")

        if df.empty:
            print("No midnight crossers found.")
            return

        # Save to CSV
        output_file = "task9_midnight_crossers.csv"
        df.to_csv(output_file, index=False)
        print(f"💾 Saved results to {output_file} ({len(df)} rows).")

        # Display first 20
        print("\nTop 20 Midnight Crossers:")
        print(tabulate(df.head(20), headers="keys", tablefmt="fancy_grid", showindex=False))

        print("\nTask 9 completed successfully.")
