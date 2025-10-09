# ------------------------------------------------------------
# Insert cleaned Porto dataset into MySQL database
# Using DbConnector.py and the cleaned CSVs
# ------------------------------------------------------------

from DbConnector import DbConnector
import pandas as pd
from mysql.connector import Error

# ------------------------------------------------------------
# Step 1. Connect to the database
# ------------------------------------------------------------
print("\n===== STEP 1: CONNECTING TO DATABASE =====")
try:
    connection = DbConnector()
    db = connection.db_connection
    cursor = connection.cursor
    print("✅ Database connection established.")
except Exception as e:
    print("❌ ERROR: Could not connect to database:", e)
    exit(1)

# ------------------------------------------------------------
# Step 2. Create tables if not exist
# ------------------------------------------------------------
print("\n===== STEP 2: CREATING TABLES =====")

try:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Trip (
            trip_id VARCHAR(50) PRIMARY KEY,
            taxi_id VARCHAR(20),
            call_type CHAR(1),
            origin_call VARCHAR(20) NULL,
            origin_stand INT NULL,
            timestamp DATETIME,
            day_type CHAR(1)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Point (
            point_id INT AUTO_INCREMENT PRIMARY KEY,
            trip_id VARCHAR(50),
            seq INT,
            latitude FLOAT,
            longitude FLOAT,
            FOREIGN KEY (trip_id) REFERENCES Trip(trip_id)
        );
    """)

    db.commit()
    print("✅ Tables Trip and Point are ready.")
except Error as e:
    print("❌ ERROR creating tables:", e)
    connection.close_connection()
    exit(1)

# ------------------------------------------------------------
# Step 3. Load data from CSVs
# ------------------------------------------------------------
print("\n===== STEP 3: LOADING CLEANED CSV FILES =====")

trips_file = "trips_clean.csv"
points_file = "points_clean.csv"

try:
    trips_df = pd.read_csv(trips_file)
    print(f"Loaded {len(trips_df)} trips from {trips_file}")
except Exception as e:
    print("❌ ERROR loading trips CSV:", e)
    connection.close_connection()
    exit(1)

# ------------------------------------------------------------
# Step 4. Insert Trip data
# ------------------------------------------------------------
print("\n===== STEP 4: INSERTING TRIP DATA =====")

trip_insert_query = """
    INSERT INTO Trip (trip_id, taxi_id, call_type, origin_call, origin_stand, timestamp, day_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

try:
    data = [tuple(x) for x in trips_df.to_numpy()]
    batch_size = 5000

    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        cursor.executemany(trip_insert_query, batch)
        db.commit()
        print(f"Inserted {i + len(batch)} / {len(data)} trips...")

    print(f"✅ Inserted all {len(trips_df)} trips into Trip table.")
except Error as e:
    print("❌ ERROR inserting trips:", e)
    db.rollback()
    connection.close_connection()
    exit(1)

# ------------------------------------------------------------
# Step 5. Insert Point data (chunked with FK check disabled)
# ------------------------------------------------------------
print("\n===== STEP 5: INSERTING POINT DATA (BATCH MODE) =====")

# Temporarily disable foreign key checks during bulk insert
cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
db.commit()

point_insert_query = """
    INSERT INTO Point (trip_id, seq, latitude, longitude)
    VALUES (%s, %s, %s, %s)
"""

try:
    chunk_size = 100000
    total_inserted = 0

    for chunk in pd.read_csv(points_file, chunksize=chunk_size):
        # Force proper Python datatypes & strip whitespaces
        data = [
            (str(r["trip_id"]).strip(), int(r["seq"]), float(r["latitude"]), float(r["longitude"]))
            for _, r in chunk.iterrows()
        ]

        cursor.executemany(point_insert_query, data)
        db.commit()

        total_inserted += len(data)
        print(f"Inserted {total_inserted:,} points...")

    print(f"✅ Finished inserting all {total_inserted:,} points into Point table.")
except Error as e:
    print("❌ ERROR inserting points:", e)
    db.rollback()
    connection.close_connection()
    exit(1)
except Exception as e:
    print("❌ Unexpected error inserting points:", e)
    db.rollback()
    connection.close_connection()
    exit(1)
finally:
    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    db.commit()

# ------------------------------------------------------------
# Step 6. Close connection
# ------------------------------------------------------------
print("\n===== STEP 6: CLOSING CONNECTION =====")
connection.close_connection()
print("✅ Database insertion completed successfully.")
