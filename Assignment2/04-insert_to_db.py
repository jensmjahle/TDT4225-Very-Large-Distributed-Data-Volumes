# ------------------------------------------------------------
# Insert cleaned Porto dataset into MySQL database
# ------------------------------------------------------------

from DbConnector import DbConnector
import pandas as pd
from mysql.connector import Error
import gc  # garbage collector

# ------------------------------------------------------------
# Step 1. Connect to the database
# ------------------------------------------------------------
print("\n===== STEP 1: CONNECTING TO DATABASE =====")
try:
    connection = DbConnector()
    db = connection.db_connection
    cursor = connection.cursor
    print("Database connection established.")
except Exception as e:
    print("ERROR: Could not connect to database:", e)
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
    print("Tables Trip and Point are ready.")
except Error as e:
    print("ERROR creating tables:", e)
    connection.close_connection()
    exit(1)

# ------------------------------------------------------------
# Step 3. Load data from CSVs
# ------------------------------------------------------------
print("\n===== STEP 3: LOADING CLEANED CSV FILES =====")

trips_file = "trips_clean.csv"
points_file = "points_clean.csv"

try:
    trips_df = pd.read_csv(trips_file, dtype=str)
    print(f"Loaded {len(trips_df):,} trips from {trips_file}")
except Exception as e:
    print("ERROR loading trips CSV:", e)
    connection.close_connection()
    exit(1)

# ------------------------------------------------------------
# Step 4. Insert Trip data (IGNORE duplicates)
# ------------------------------------------------------------
print("\n===== STEP 4: INSERTING TRIP DATA (IGNORE duplicates) =====")

trip_insert_query = """
    INSERT IGNORE INTO Trip (trip_id, taxi_id, call_type, origin_call, origin_stand, timestamp, day_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

try:
    data = [tuple(x) for x in trips_df.to_numpy()]
    batch_size = 5000
    total = len(data)

    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        cursor.executemany(trip_insert_query, batch)
        db.commit()
        print(f"Inserted {i + len(batch):,} / {total:,} trips...")

    print(f"Inserted all {total:,} trips into Trip table (duplicates ignored).")
except Error as e:
    print("ERROR inserting trips:", e)
    db.rollback()
    connection.close_connection()
    exit(1)

# ------------------------------------------------------------
# Step 5. Insert Point data (chunked, IGNORE + memory safe)
# ------------------------------------------------------------
print("\n===== STEP 5: INSERTING POINT DATA (CHUNKED + MEMORY SAFE) =====")

cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
db.commit()

point_insert_query = """
    INSERT IGNORE INTO Point (trip_id, seq, latitude, longitude)
    VALUES (%s, %s, %s, %s)
"""

def normalize_trip_id(trip_id):
    try:
        if pd.isna(trip_id):
            return None
        s = str(trip_id).strip()
        if "e" in s or "E" in s or "." in s:
            s = str(int(float(s)))
        return s
    except Exception:
        return str(trip_id)

try:
    chunk_size = 50000
    total_inserted = 0

    for chunk in pd.read_csv(points_file, chunksize=chunk_size):
        chunk["trip_id"] = chunk["trip_id"].apply(normalize_trip_id)

        if chunk.empty:
            continue

        data = [
            (str(r["trip_id"]), int(r["seq"]), float(r["latitude"]), float(r["longitude"]))
            for _, r in chunk.iterrows()
        ]

        cursor.executemany(point_insert_query, data)
        db.commit()
        total_inserted += len(data)
        print(f"Inserted {total_inserted:,} points...")

        del chunk, data
        gc.collect()

    print(f"Finished inserting {total_inserted:,} points into Point table (duplicates ignored).")
except Error as e:
    print("ERROR inserting points:", e)
    db.rollback()
    connection.close_connection()
    exit(1)
except Exception as e:
    print("Unexpected error inserting points:", e)
    db.rollback()
    connection.close_connection()
    exit(1)
finally:
    try:
        if db.is_connected():
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            db.commit()
    except Exception:
        pass

# ------------------------------------------------------------
# Step 6. Verification summary
# ------------------------------------------------------------
print("\n===== STEP 6: VERIFYING DATABASE COUNTS =====")
try:
    cursor.execute("SELECT COUNT(*) FROM Trip;")
    trips_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM Point;")
    points_count = cursor.fetchone()[0]
    print(f"Database now contains {trips_count:,} trips and {points_count:,} points.")
except Exception as e:
    print("Verification failed:", e)

# ------------------------------------------------------------
# Step 7. Close connection
# ------------------------------------------------------------
print("\n===== STEP 7: CLOSING CONNECTION =====")
connection.close_connection()
print("Database insertion completed successfully (FULL LOAD, duplicates ignored).")
