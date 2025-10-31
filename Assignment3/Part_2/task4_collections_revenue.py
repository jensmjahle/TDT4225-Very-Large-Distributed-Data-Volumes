# ------------------------------------------------------------
# TASK 4: Top 10 Film Collections by Total Revenue
# ------------------------------------------------------------
from print_helper import print_table

class Task4:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 4: Top 10 Film Collections by Total Revenue ---")

        pipeline = [
            # Only collections that exist and have a name
            {"$match": {
                "belongs_to_collection.name": {"$exists": True, "$ne": None}
            }},
            # Group by collection name
            {"$group": {
                "_id": "$belongs_to_collection.name",
                "movie_count": {"$sum": 1},
                "total_revenue": {"$sum": "$revenue"},
                "vote_list": {"$push": "$vote_average"},
                "earliest_release_date": {"$min": "$release_date"},
                "latest_release_date": {"$max": "$release_date"}
            }},
            #  Calculate median vote_average
            {"$addFields": {
                "sorted_votes": {"$sortArray": {"input": "$vote_list", "sortBy": 1}},
                "mid_index": {"$floor": {"$divide": [{"$size": "$vote_list"}, 2]}}
            }},
            {"$addFields": {
                "median_vote_average": {
                    "$cond": [
                        {"$eq": [{"$mod": [{"$size": "$sorted_votes"}, 2]}, 0]},
                        {
                            "$avg": [
                                {"$arrayElemAt": ["$sorted_votes", {"$subtract": ["$mid_index", 1]}]},
                                {"$arrayElemAt": ["$sorted_votes", "$mid_index"]}
                            ]
                        },
                        {"$arrayElemAt": ["$sorted_votes", "$mid_index"]}
                    ]
                }
            }},
            # Keep only needed fields
            {"$project": {
                "_id": 0,
                "collection_name": "$_id",
                "movie_count": 1,
                "total_revenue": 1,
                "median_vote_average": 1,
                "earliest_release_date": 1,
                "latest_release_date": 1
            }},
            # Filter: collections with ≥3 movies
            {"$match": {"movie_count": {"$gte": 3}}},
            # Sort by total_revenue desc
            {"$sort": {"total_revenue": -1}},
            {"$limit": 10}
        ]

        results = list(self.db.movies.aggregate(pipeline))
        print_table(
            results,
            headers=[
                "collection_name",
                "movie_count",
                "total_revenue",
                "median_vote_average",
                "earliest_release_date",
                "latest_release_date",
            ],
        )
        print(f"\nReturned {len(results)} collections.")
