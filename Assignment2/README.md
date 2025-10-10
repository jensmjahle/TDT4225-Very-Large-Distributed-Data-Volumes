## Task 1
1. Download and place the `porto.csv` file in the root directory.
2. Run the following command to execute the EDA:
   ```bash
   python 01-eda.py
   ```
3. The EDA results will be printed to the terminal.
4. Run the following command to preprocess the data and generate a porto_preprocessed.csv file:
   ```bash
   python 02-preprocess_data.py
   ```
5. The preprocessed data will be saved as `porto_preprocessed.csv` in the root directory.
6. Run the following command to execute the preparation for database script:
   ```bash
   python 03-prepare_for_db.py
   ```
7. This scripts creates to files `trips_clean.csv` and `points_clean.csv` in the root directory. These files are ready to be imported into a database.
8. Run the following command to import the cleaned data into a MySQL database:
   ```bash
   python 04-insert_to_db.py
   ```
9. Ensure you have a MySQL database set up and the connection details are correctly configured in the `DbConnector.py` file.


## Task 2
1. Navigate to the `task2` directory:
   ```bash
   cd task2
   ```
2. Run the following command to execute the tasks:
   ```bash
    python run_tasks.py
    ```
3. Note that every task is turned off by default. To enable a specific task, uncomment the corresponding line in the `run_tasks.py` file.
4. Ensure that the database connection details in the `DbConnector.py` file are correctly configured to connect to your MySQL database.
5. The results of each task will be printed to the terminal when executed. And some tasks will generate csv files in the `task2` directory.