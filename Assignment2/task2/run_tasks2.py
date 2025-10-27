# ------------------------------------------------------------
# TDT4225 - Assignment 2, Part 2
# ------------------------------------------------------------
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DbConnector import DbConnector
from helpers.sql_runner import SQLRunner
from helpers.task4a_helper import Task4AHelper
from helpers.task4b_helper import Task4BHelper
from helpers.task5_helper import Task5Helper
from helpers.task6_helper import Task6Helper
from helpers.task8_helper import Task8Helper
from helpers.task9_helper import Task9Helper
from helpers.task10_helper import Task10Helper
from helpers.task11_helper import Task11Helper

class TaskRunner:
    def __init__(self, sql_folder="sql_tasks"):
        self.sql_folder = sql_folder
        self.connection = DbConnector()
        self.db = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.runner = SQLRunner(self.cursor, sql_folder)

    def run_all(self):
        print("\n=== Starting Assignment 2: Part 2 Tasks ===")

        # Task 1
        print("\n==== Running Task 1 ====")
        #self.runner.run_sql("task1_counts.sql")

        # Task 2
        print("\n==== Running Task 2 ====")
        self.runner.run_sql("task2_avg_trips.sql")

        # Task 3
        print("\n==== Running Task 3 ====")
        #self.runner.run_sql("task3_top20_taxis.sql")

        # Task 4a
        print("\n==== Running Task 4a ====")
        #task4a = Task4AHelper(self.runner.cursor)
        #task4a.run_task4a()

        # Task 4b
        print("\n==== Running Task 4b ====")
        #task4b = Task4BHelper(self.cursor, self.sql_folder)
        #task4b.run_task4b()

        # Task 5
        print("\n==== Running Task 5 ====")
        #task5 = Task5Helper(self.cursor, self.db, self.sql_folder)
        #task5.run_task5(chunk_size=50)


        # Task 6
        print("\n==== Running Task 6 ====")
        #task6 = Task6Helper(self.runner.cursor)
        #task6.run_task6()

        # Task 7
        print("\n==== Running Task 7 ====")
        #self.runner.run_sql("task7_invalid_trips.sql")

        # Task 8
        print("\n==== Running Task 8 ====")
        #task8 = Task8Helper(self.runner.cursor, self.db)
        #task8.run_task8(limit_pairs=50000, chunk_size=5000)

        # Task 9
        print("\n==== Running Task 9 ====")
        #task9 = Task9Helper(self.db.cursor(), self.sql_folder)
        #task9.run_task9()

        # Task 10
        print("\n==== Running Task 10 ====")
        #task10 = Task10Helper(self.db.cursor)
        #task10.run_task10()

        # Task 11
        print("\n==== Running Task 11 ====")
        task11 = Task11Helper(self.db.cursor(), self.sql_folder)
        task11.run_task11()

        print("\n=== All tasks executed successfully ===")

    def close(self):
        self.connection.close_connection()
        print("Database connection closed.")


if __name__ == "__main__":
    runner = TaskRunner()
    runner.run_all()
    runner.close()
