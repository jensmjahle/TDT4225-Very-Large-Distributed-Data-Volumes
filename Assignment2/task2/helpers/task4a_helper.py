
import os
import pandas as pd
from tabulate import tabulate


class Task4AHelper:
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

    def run_task4a(self):
        """Run Task 4a and save results."""
        print("\n--- TASK 4a: MOST USED CALL TYPE PER TAXI ---")

        df = self._run_sql_file("task4a_calltype_per_taxi.sql")

        if df.empty:
            print("No data available.")
            return

        output_file = "task4a_most_used_calltype.csv"
        df.to_csv(output_file, index=False)
        print(f"Saved results to {output_file} ({len(df)} rows).")

        print("\nTop 20 Taxis by Trip Count:")
        print(
            tabulate(
                df.head(20),
                headers="keys",
                tablefmt="fancy_grid",
                showindex=False,
            )
        )

        print("\nTask 4a completed successfully.")
