# ------------------------------------------------------------
# TDT4225 - Assignment 3, Part 2 (MongoDB)
# ------------------------------------------------------------
import sys
import os
from pathlib import Path
from pprint import pprint

CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
sys.path.append(str(PARENT_DIR))

from DbConnector import DbConnector
from task1_directors import Task1


class MongoTaskRunner:
    def __init__(self):
        self.conn = DbConnector()
        self.db = self.conn.db

    def run_all(self):
        print("\n=== Starting Assignment 3: Part 2 Tasks ===")

        print("Running task 1")
        Task1(self.db).run()
        print("Task 1 completed")
        #Task2(self.db).run()

        print("\n=== All MongoDB tasks executed successfully ===")

    def close(self):
        self.conn.close_connection()
        print("Database connection closed.")

if __name__ == "__main__":
    runner = MongoTaskRunner()
    runner.run_all()
    runner.close()
