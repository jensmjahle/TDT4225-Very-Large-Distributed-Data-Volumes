# ------------------------------------------------------------
# TDT4225 - Assignment 2, Part 2
# Run all SQL tasks using DbConnector and tabulate for output
# ------------------------------------------------------------

from DbConnector import DbConnector
import pandas as pd
import os
from tabulate import tabulate

class TaskRunner:
    def __init__(self, sql_folder="sql_tasks"):
        self.sql_folder = sql_folder
        self.connection = DbConnector()
        self.db = self.connection.db_connection
        self.cursor = self.connection.cursor

    def run_sql(self, filename):
        """Execute a single SQL file and display the results with tabulate."""
        filepath = os.path.join(self.sql_folder, filename)
        print(f"\n===== Running {filename} =====")

        with open(filepath, "r", encoding="utf-8") as f:
            query = f.read().strip()

        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            columns = self.cursor.column_names

            if not rows:
                print("⚠️ No results returned.")
                return pd.DataFrame()

            df = pd.DataFrame(rows, columns=columns)
            print(tabulate(df, headers="keys", tablefmt="fancy_grid", showindex=False))
            return df
        except Exception as e:
            print(f"❌ Error running {filename}: {e}")
            return pd.DataFrame()

    def run_all(self):
        """Run all SQL files in sorted order."""
        sql_files = sorted([f for f in os.listdir(self.sql_folder) if f.endswith(".sql")])
        if not sql_files:
            print("⚠️ No .sql files found in", self.sql_folder)
            return

        results = {}
        for file in sql_files:
            results[file] = self.run_sql(file)

        print("\n=== All tasks executed successfully ===")
        return results

    def close(self):
        self.connection.close_connection()
        print("✅ Database connection closed.")


if __name__ == "__main__":
    runner = TaskRunner()
    results = runner.run_all()
    runner.close()
