# ------------------------------------------------------------
# Exploratory Data Analysis (EDA)
# TDT4225 - Very Large, Distributed Data Volumes
# Porto Taxi Trajectory Dataset
# ------------------------------------------------------------
from collections import Counter

import numpy as np
from collections import Counter
import datashader as ds
import datashader.transfer_functions as tf
from PIL._imaging import display
from datashader.utils import export_image
import colorcet as cc
import matplotlib.pyplot as plt
from tabulate import tabulate
import json
import time
import pandas as pd
import ast
import matplotlib.pyplot as plt
import seaborn as sns
from math import radians, sin, cos, sqrt, atan2
from matplotlib.colors import LinearSegmentedColormap


# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------
def shorten_polyline(x, max_chars=40):
    """Shorten POLYLINE string for display."""
    if isinstance(x, str):
        return (x[:max_chars] + "...") if len(x) > max_chars else x
    return x

def pretty_print(df, title=None, n=10):
    """Nicely formatted DataFrame print with truncated POLYLINE."""
    if df is None or df.empty:
        print(f"\n{title or 'Table'}: (no data)")
        return
    if title:
        print(f"\n{title}")
    df_display = df.copy()
    if "POLYLINE" in df_display.columns:
        df_display["POLYLINE"] = df_display["POLYLINE"].apply(shorten_polyline)
    print(tabulate(df_display.head(n), headers='keys', tablefmt='fancy_grid'))
    if len(df_display) > n:
        print(f"... ({len(df_display) - n} more rows)")

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    phi1, phi2 = radians(lat1), radians(lat2)
    a = sin(dphi / 2)**2 + cos(phi1) * cos(phi2) * sin(dlambda / 2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))

# ------------------------------------------------------------
# Step 1. Loading the Dataset
# ------------------------------------------------------------
print("\n===== STEP 1: LOADING THE DATASET =====")
file_path = "porto.csv"   # Change this to your actual dataset filename
df = pd.read_csv(file_path)

print(f"Dataset loaded successfully! Shape: {df.shape}")
pretty_print(df.head(), "First 5 rows")

# ------------------------------------------------------------
# Step 2. Understanding the Data
# ------------------------------------------------------------
print("\n===== STEP 2: BASIC INFORMATION =====")
print(df.info())

print("\nData Types:")
pretty_print(pd.DataFrame(df.dtypes, columns=['Type']))

print("\nSummary Statistics:")
pretty_print(df.describe(include="all").transpose())

# ------------------------------------------------------------
# Step 3. Checking for Missing or Null Values
# ------------------------------------------------------------
print("\n===== STEP 3: MISSING VALUE ANALYSIS =====")
missing_counts = df.isnull().sum()
missing_percent = round(df.isnull().mean() * 100, 2)
missing_df = pd.DataFrame({"Missing Values": missing_counts, "Percent (%)": missing_percent})
pretty_print(missing_df, "Missing Value Overview")

# ------------------------------------------------------------
# Step 4. Explore Unique Values and Structure
# ------------------------------------------------------------
print("\n===== STEP 4: UNIQUE VALUES AND STRUCTURE =====")

for col in ["CALL_TYPE", "DAYTYPE", "MISSING_DATA"]:
    if col in df.columns:
        pretty_print(pd.DataFrame(df[col].value_counts()), f"Unique values in {col}")

if "TAXI_ID" in df.columns:
    print(f"\nNumber of unique taxis: {df['TAXI_ID'].nunique()}")

# Trip ID duplication analysis
if "TRIP_ID" in df.columns:
    num_unique_trips = df["TRIP_ID"].nunique()
    num_total_trips = df.shape[0]
    num_duplicate_trips = num_total_trips - num_unique_trips

    print(f"\nNumber of unique trips: {num_unique_trips}")
    print(f"Number of duplicate trips: {num_duplicate_trips}")

    trip_counts = df["TRIP_ID"].value_counts()
    duplicate_trip_counts = trip_counts[trip_counts > 1]

    pretty_print(duplicate_trip_counts.reset_index().rename(columns={'index': 'TRIP_ID', 'TRIP_ID': 'Count'}),
                 "TRIP_IDs with duplicates")

    duplicate_trip_counts2 = trip_counts[trip_counts > 2]
    if not duplicate_trip_counts2.empty:
        pretty_print(duplicate_trip_counts2.reset_index().rename(columns={'index': 'TRIP_ID', 'TRIP_ID': 'Count'}),
                     "TRIP_IDs with more than 2 duplicates")

    if num_duplicate_trips > 0:
        duplicate_rows = df[df.duplicated(subset=["TRIP_ID"], keep=False)]
        pretty_print(duplicate_rows, "Duplicate TRIP_ID entries")

# Check duplicates by TAXI_ID and TIMESTAMP
if {"TAXI_ID", "TIMESTAMP"}.issubset(df.columns):
    duplicates_mask = df.duplicated(subset=["TAXI_ID", "TIMESTAMP"], keep=False)
    num_duplicates = duplicates_mask.sum()
    print(f"\nNumber of trips that are duplicates (same TAXI_ID and TIMESTAMP): {num_duplicates}")

    if num_duplicates > 0:
        pretty_print(df[duplicates_mask], "Example duplicate trips (same TAXI_ID & TIMESTAMP)")

# Fully identical rows
duplicate_rows_all = df[df.duplicated(keep=False)]
num_duplicate_rows_all = duplicate_rows_all.shape[0]
print(f"\nNumber of fully duplicate rows: {num_duplicate_rows_all}")
if num_duplicate_rows_all > 0:
    pretty_print(duplicate_rows_all, "Fully Duplicate Rows")

# ------------------------------------------------------------
# Step 4b. Logical Consistency Checks for CALL_TYPE / ORIGIN_CALL / ORIGIN_STAND
# ------------------------------------------------------------
print("\n===== STEP 4b: LOGICAL CONSISTENCY CHECKS =====")

invalid_conditions = []

# Rule 1: If CALL_TYPE == 'A', ORIGIN_CALL should NOT be null
invalid_A = df[(df["CALL_TYPE"] == "A") & (df["ORIGIN_CALL"].isnull())]
invalid_conditions.append(("CALL_TYPE = 'A' but ORIGIN_CALL is NULL", invalid_A.shape[0]))

# Rule 2: If CALL_TYPE == 'A', ORIGIN_STAND should be null
invalid_A2 = df[(df["CALL_TYPE"] == "A") & (df["ORIGIN_STAND"].notnull())]
invalid_conditions.append(("CALL_TYPE = 'A' but ORIGIN_STAND is NOT NULL", invalid_A2.shape[0]))

# Rule 3: If CALL_TYPE == 'B', ORIGIN_STAND should NOT be null
invalid_B = df[(df["CALL_TYPE"] == "B") & (df["ORIGIN_STAND"].isnull())]
invalid_conditions.append(("CALL_TYPE = 'B' but ORIGIN_STAND is NULL", invalid_B.shape[0]))

# Rule 4: If CALL_TYPE == 'B', ORIGIN_CALL should be null
invalid_B2 = df[(df["CALL_TYPE"] == "B") & (df["ORIGIN_CALL"].notnull())]
invalid_conditions.append(("CALL_TYPE = 'B' but ORIGIN_CALL is NOT NULL", invalid_B2.shape[0]))

# Rule 5: If CALL_TYPE == 'C', both ORIGIN_CALL and ORIGIN_STAND should be null
invalid_C = df[(df["CALL_TYPE"] == "C") & ((df["ORIGIN_CALL"].notnull()) | (df["ORIGIN_STAND"].notnull()))]
invalid_conditions.append(("CALL_TYPE = 'C' but ORIGIN_CALL or ORIGIN_STAND is NOT NULL", invalid_C.shape[0]))

# Combine and print results
consistency_df = pd.DataFrame(invalid_conditions, columns=["Violation Description", "Count"])
print(tabulate(consistency_df, headers='keys', tablefmt='fancy_grid'))

# Optional: print some examples of invalid rows (if any)
invalid_total = sum(c for _, c in invalid_conditions)
print(f"\nTotal inconsistent rows found: {invalid_total}")

if invalid_total > 0:
    sample_invalids = pd.concat([invalid_A, invalid_A2, invalid_B, invalid_B2, invalid_C]).head(10)
    pretty_print(sample_invalids, "Example inconsistent rows (first 10)")


# ------------------------------------------------------------
# Step 5. Explore POLYLINE Structure
# ------------------------------------------------------------
print("\n===== STEP 5: POLYLINE STRUCTURE =====")
pretty_print(df[["POLYLINE"]].head(3).reset_index(), "Example POLYLINE values (truncated)")

sample_rows = df["POLYLINE"].head(5).apply(lambda x: ast.literal_eval(x))
for i, coords in enumerate(sample_rows):
    print(f"\nTrip {i+1} has {len(coords)} GPS points")
    if len(coords) > 0:
        print("  First point:", coords[0])
        print("  Last point:", coords[-1])

# ------------------------------------------------------------
# Step 6. Basic Exploration and Patterns
# ------------------------------------------------------------
print("\n===== STEP 6: BASIC EXPLORATION =====")

if "TAXI_ID" in df.columns:
    taxi_counts = df["TAXI_ID"].value_counts().head(5)
    pretty_print(taxi_counts.reset_index().rename(columns={'index': 'TAXI_ID', 'TAXI_ID': 'Trips'}),
                 "Top 5 Taxis with Most Trips")

for col in ["CALL_TYPE", "DAY_TYPE", "MISSING_DATA"]:
    if col in df.columns:
        pretty_print(df[col].value_counts().reset_index().rename(columns={'index': col, col: 'Count'}),
                     f"Distribution of {col}")

print("\n=== EDA COMPLETED SUCCESSFULLY ===")


# ------------------------------------------------------------
# Step 7: Parse POLYLINE only once
# ------------------------------------------------------------


#df = pd.read_pickle("parsed.pkl")


print("\n===== STEP 7: PARSING POLYLINES =====")
start_time = time.time()
parsed_polylines = []
n = len(df)
progress_intervals = 10
checkpoint = max(n // progress_intervals, 1)

for i, polyline_str in enumerate(df["POLYLINE"]):
    try:
        coords = json.loads(polyline_str)
        parsed_polylines.append(coords if isinstance(coords, list) else [])
    except:
        parsed_polylines.append([])
    if (i + 1) % checkpoint == 0 or (i + 1) == n:
        progress = int(((i + 1) / n) * 100)
        elapsed = time.time() - start_time
        print(f"Parsing POLYLINE: {progress}% ({i+1}/{n}) | Elapsed: {elapsed:.1f}s")

df["POLYLINE_LIST"] = parsed_polylines
df.to_pickle("parsed.pkl")


# ------------------------------------------------------------
# Step 8: Compute Distance and Duration
# ------------------------------------------------------------
def compute_trip_metrics_from_list(coords):
    if len(coords) < 2:
        return 0.0, len(coords) * 15
    dist = 0.0
    for i in range(len(coords) - 1):
        lon1, lat1 = coords[i]
        lon2, lat2 = coords[i + 1]
        dist += haversine_distance(lat1, lon1, lat2, lon2)
    duration = len(coords) * 15
    return dist, duration

print("\n===== STEP 8: CALCULATING DISTANCE AND DURATION =====")
distances = []
durations = []
start_time = time.time()

for i, coords in enumerate(df["POLYLINE_LIST"]):
    dist, dur = compute_trip_metrics_from_list(coords)
    distances.append(dist)
    durations.append(dur)
    if (i + 1) % checkpoint == 0 or (i + 1) == n:
        progress = int(((i + 1) / n) * 100)
        elapsed = time.time() - start_time
        print(f"Progress: {progress}% ({i+1}/{n}) | Elapsed: {elapsed:.1f}s")

df["distance_km"] = distances
df["duration_sec"] = durations
print("Distance and duration computed successfully!")
pretty_print(df[["TRIP_ID", "distance_km", "duration_sec"]].head())

# ------------------------------------------------------------
# Step 9: Scatter Plot (Distance vs Duration, log-log)
# ------------------------------------------------------------

# Filter rows with missing data
missing_df = df[df["MISSING_DATA"] == True].copy()

# Expand POLYLINE_LIST into individual (lon, lat, trip_id) points
expanded_points = []
for trip_id, poly in zip(missing_df["TRIP_ID"], missing_df["POLYLINE_LIST"]):
    if isinstance(poly, list) and len(poly) > 0:
        for lon, lat in poly:
            expanded_points.append((trip_id, lon, lat))

# Create a DataFrame for plotting
points_df = pd.DataFrame(expanded_points, columns=["TRIP_ID", "lon", "lat"])

# Plot the scatterplot
plt.figure(figsize=(10, 8))
sns.scatterplot(
    data=points_df,
    x="lon",
    y="lat",
    hue="TRIP_ID",
    palette="tab20",
    s=10,
    alpha=0.7,
    legend=False  # hide legend if too many trips
)
plt.title("GPS Points for Trips with Missing Data")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.grid(True, linestyle="--", alpha=0.5)
plt.show()

expanded_points = []
for trip_id, poly in zip(df["TRIP_ID"], df["POLYLINE_LIST"]):
    if isinstance(poly, list) and len(poly) > 0:
        for lon, lat in poly:
            expanded_points.append((trip_id, lon, lat))

# Create a DataFrame for plotting
points_df = pd.DataFrame(expanded_points, columns=["TRIP_ID", "lon", "lat"])

# Plot the scatterplot
plt.figure(figsize=(10, 8))
sns.scatterplot(
    data=points_df,
    x="lon",
    y="lat",
    hue="TRIP_ID",
    palette="tab20",  # use different colors for trips
    s=10,
    alpha=0.6,
    legend=False  # hide legend to avoid clutter
)
plt.title("All Trips: GPS Points Colored by Trip ID")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.grid(True, linestyle="--", alpha=0.3)
plt.show()

plt.figure(figsize=(10, 6))
plot_df = df[(df["distance_km"] > 0) & (df["duration_sec"] > 0)]

sns.scatterplot(
    data=plot_df[plot_df["MISSING_DATA"] == False],
    x="duration_sec", y="distance_km", color="blue", alpha=0.5, s=20, label="Complete Data"
)
# sns.scatterplot(
#     data=plot_df[plot_df["MISSING_DATA"] == True],
#     x="duration_sec", y="distance_km", color="red", alpha=0.7, s=30, label="Missing Data"
# )

# Logarithmic scale
plt.xscale("log")
plt.yscale("log")
plt.title("Trip Distance vs Duration (Log–Log, Missing Data Highlighted)")
plt.xlabel("Duration (seconds, log scale)")
plt.ylabel("Distance (km, log scale)")
plt.legend(loc="lower right", bbox_to_anchor=(1.0, 0.0))
plt.grid(True, which="both", linestyle="--", linewidth=0.5)
plt.show()

# ------------------------------------------------------------
# Step 10: Route Heatmap (frequent GPS points)
# ------------------------------------------------------------


# --- Assumptions: ---
# - df["POLYLINE_LIST"] exists and is a list-of-lists of [lon, lat] pairs
# - df["MISSING_DATA"] exists (boolean)
# - If POLYLINE_LIST is not present, parse once earlier (json.loads/ast.literal_eval)

# 1) Quick check
if "POLYLINE_LIST" not in df.columns:
    raise RuntimeError("You must have df['POLYLINE_LIST'] parsed already.")

# 2) Compute bounding box and total points (one scan; no heavy memory)
print("Computing bounds and total point count...")
t0 = time.time()
min_lon = 1e9
max_lon = -1e9
min_lat = 1e9
max_lat = -1e9
total_points = 0

for poly in df["POLYLINE_LIST"]:
    if not poly:
        continue
    # poly is list of [lon, lat]
    lons = [p[0] for p in poly]
    lats = [p[1] for p in poly]
    if min(lons) < min_lon: min_lon = min(lons)
    if max(lons) > max_lon: max_lon = max(lons)
    if min(lats) < min_lat: min_lat = min(lats)
    if max(lats) > max_lat: max_lat = max(lats)
    total_points += len(poly)

print(f"Bounds: lon [{min_lon:.6f}, {max_lon:.6f}], lat [{min_lat:.6f}, {max_lat:.6f}]")
print(f"Total GPS points (approx): {total_points:,} - time {time.time()-t0:.1f}s")

# 3) Create a datashader Canvas
# choose resolution that suits you (higher = more detail, slower)
plot_width, plot_height = 24000, 18000
cvs = ds.Canvas(plot_width=plot_width,
                plot_height=plot_height,
                x_range=(min_lon, max_lon),
                y_range=(min_lat, max_lat))

# 4) Aggregate in chunks to avoid building a huge intermediate DataFrame
# We'll sum partial aggregations into `agg_total`.
chunk_trip_count = 5000   # tune: how many trips' points to process per chunk
agg_total = None
n = len(df)
t0 = time.time()
chunk = []
count_trips = 0

print("Aggregating points into raster (chunked) ...")
for i, poly in enumerate(df["POLYLINE_LIST"]):
    if poly:
        # extend chunk with tuples (lon, lat)
        for lon, lat in poly:
            chunk.append((lon, lat))

    count_trips += 1
    if (count_trips % chunk_trip_count == 0) or (i == n - 1):
        # convert to DataFrame (smallish chunk)
        if chunk:
            chunk_df = pd.DataFrame(chunk, columns=["lon", "lat"])
            partial = cvs.points(chunk_df, 'lon', 'lat', ds.count())
            if agg_total is None:
                agg_total = partial
            else:
                agg_total = agg_total + partial
            chunk = []  # reset chunk
        # progress print
        progress = (i + 1) / n * 100
        print(f"  processed trips {(i+1):,}/{n:,} ({progress:.1f}%), elapsed {time.time()-t0:.1f}s")

# if no points at all
if agg_total is None:
    raise RuntimeError("No GPS points were aggregated - check POLYLINE_LIST content")

# 5) Colorize the aggregated image
cmap = LinearSegmentedColormap.from_list('black_orange_yellow', ['black', 'orange', 'yellow'])

# 2) Shade the aggregated data with custom colormap and histogram equalization
img = tf.shade(agg_total, cmap=cmap, how='eq_hist')

# 3) Spread thin routes slightly
img2 = tf.dynspread(img, threshold=0.5, max_px=3)

# 4) Set black background
img2 = tf.set_background(img2, "black")

# 5) Display
plt.figure(figsize=(10,8))
plt.imshow(img2.to_pil())
plt.axis('off')
plt.title("Datashader Taxi Route Density Heatmap (Black-Orange-Yellow)")
plt.show()

# # 7) OPTIONAL: overlay some MISSING_DATA points (sampled) on top using matplotlib
# missing_pts = []
# sample_rate = 0.005  # adjust as needed; small sample for speed
# for poly, missing in zip(df["POLYLINE_LIST"], df["MISSING_DATA"]):
#     if missing and poly:
#         # sample some points from the route to plot (avoid plotting millions)
#         for lon, lat in poly[::max(1, int(1/sample_rate))]:
#             missing_pts.append((lon, lat))
#
# if missing_pts:
#     xs, ys = zip(*missing_pts)
#     plt.figure(figsize=(10,8))
#     plt.imshow(img2.to_pil())
#     plt.scatter(
#         [(x - min_lon) / (max_lon - min_lon) * plot_width for x in xs],
#         [(y - min_lat) / (max_lat - min_lat) * plot_height for y in ys],
#         s=4, c='red', alpha=0.6, label='Missing Data (sampled)'
#     )
#     plt.legend()
#     plt.axis('off')
#     plt.title("Heatmap with sampled MISSING_DATA overlay")
#     plt.show()
