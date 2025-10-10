
import pandas as pd
import os
from tabulate import tabulate

class SQLRunner:
    def __init__(self, cursor, sql_folder="sql_tasks"):
        self.cursor = cursor
        self.sql_folder = sql_folder

    def run_sql(self, filename):
        filepath = os.path.join(self.sql_folder, filename)

        with open(filepath, "r", encoding="utf-8") as f:
            query = f.read().strip()

        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            columns = self.cursor.column_names

            if not rows:
                print("No results returned.")
                return pd.DataFrame()

            df = pd.DataFrame(rows, columns=columns)
            print(tabulate(df, headers="keys", tablefmt="fancy_grid", showindex=False))
            return df
        except Exception as e:
            print(f"Error running {filename}: {e}")
            return pd.DataFrame()
