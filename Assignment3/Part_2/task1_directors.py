from print_helper import print_table

class Task1:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 1: Top 10 Directors by Median Revenue ---")

        pipeline = [
            {"$unwind": "$crew"},
            {"$match": {"crew.job": "Director"}},
            {"$group": {
                "_id": "$crew.name",
                "movie_count": {"$sum": 1},
                "revenues": {"$push": "$revenue"},
                "avg_vote": {"$avg": "$vote_average"}
            }},
            {"$match": {"movie_count": {"$gte": 5}}},
            {"$project": {
                "director": "$_id",
                "movie_count": 1,
                "avg_vote": 1,
                "median_revenue": {
                    "$function": {
                        "body": """
                        function(arr) {
                            arr = arr.filter(x => typeof x === 'number' && !isNaN(x));
                            if (arr.length === 0) return null;
                            arr.sort((a, b) => a - b);
                            var mid = Math.floor(arr.length / 2);
                            return arr.length % 2
                                ? arr[mid]
                                : (arr[mid - 1] + arr[mid]) / 2;
                        }
                        """,
                        "args": ["$revenues"],
                        "lang": "js"
                    }
                }
            }},
            {"$sort": {"median_revenue": -1}},
            {"$limit": 10}
        ]

        results = list(self.db.movies.aggregate(pipeline))
        print_table(results, headers=["director", "movie_count", "avg_vote", "median_revenue"])
        print(f"\nReturned {len(results)} directors.")
