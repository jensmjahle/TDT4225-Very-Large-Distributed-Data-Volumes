# ------------------------------------------------------------
# TASK 5: Median Runtime by Decade and Primary Genre
# ------------------------------------------------------------
from print_helper import print_table

class Task5:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 5: Median Runtime by Decade and Primary Genre ---")

        pipeline = [
            # Derive year from release_date (string → date)
            {"$addFields": {
                "release_dt": {
                    "$dateFromString": {
                        "dateString": "$release_date",
                        "onError": None,
                        "onNull": None
                    }
                }
            }},
            {"$addFields": {
                "year": {"$year": "$release_dt"}
            }},
            # Valid fields only
            {"$match": {
                "year": {"$ne": None},
                "runtime": {"$ne": None, "$gt": 0},
                "genres": {"$exists": True, "$ne": []}
            }},
            # Compute decade and primary genre
            {"$project": {
                "decade": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]},
                "primary_genre": {"$arrayElemAt": ["$genres.name", 0]},
                "runtime": 1
            }},
            # Group by decade + primary_genre
            {"$group": {
                "_id": {"decade": "$decade", "genre": "$primary_genre"},
                "runtimes": {"$push": "$runtime"},
                "movie_count": {"$sum": 1}
            }},
            # Compute median runtime
            {"$addFields": {
                "sorted_runtimes": {"$sortArray": {"input": "$runtimes", "sortBy": 1}},
                "mid_index": {"$floor": {"$divide": [{"$size": "$runtimes"}, 2]}}
            }},
            {"$addFields": {
                "median_runtime": {
                    "$cond": [
                        {"$eq": [{"$mod": [{"$size": "$sorted_runtimes"}, 2]}, 0]},
                        {
                            "$avg": [
                                {"$arrayElemAt": ["$sorted_runtimes", {"$subtract": ["$mid_index", 1]}]},
                                {"$arrayElemAt": ["$sorted_runtimes", "$mid_index"]}
                            ]
                        },
                        {"$arrayElemAt": ["$sorted_runtimes", "$mid_index"]}
                    ]
                }
            }},
            # Project clean result
            {"$project": {
                "_id": 0,
                "decade": "$_id.decade",
                "primary_genre": "$_id.genre",
                "movie_count": 1,
                "median_runtime": 1
            }},
            # Sort by decade, then median_runtime desc
            {"$sort": {"decade": 1, "median_runtime": -1}}
        ]

        results = list(self.db.movies.aggregate(pipeline))

        print_table(
            results,
            headers=["decade", "primary_genre", "movie_count", "median_runtime"]
        )
        print(f"\nReturned {len(results)} decade–genre groups.")
