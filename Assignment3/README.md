# Assignment 3: Data Analysis and Visualization

## Part 1: EDA, Data Cleaning, and Preprocessing
> It is important that you run the scripts in the order provided below to ensure proper data handling and processing.

1. Run the eda notebook to perform exploratory data analysis on the dataset.
```
jupyter notebook Part_1/01-eda.ipynb
```

2. Clean and preprocess the data using 02-preprocess_data.py
```
python Part_1/02-preprocess_data.py
```
3. Prepare the cleaned data for database insertion using 03-prepare_for_db.py
```
python Part_1/03-prepare_for_db.py
```
4. Insert the cleaned data into the database using 04-insert_to_mongo.py
```
python Part_1/04-insert_to_mongo.py
```

## Part 2: Database Queries
> After inserting the data into the database, run the following scripts to perform various queries and analyses.
1. Run the database queries using run_tasks_mongo.py in the Part_2 directory.
```
python Part_2/run_tasks_mongo.py
```
>This scrip runs all tasks, if you only want to run specific tasks, you can comment out the others in the script.
