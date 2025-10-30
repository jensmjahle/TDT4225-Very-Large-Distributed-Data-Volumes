from print_helper import print_table

class Task8:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 8: Top 20 Director–Actor Collaborations ---")

        pipeline = [
            # Only movies with sufficient votes
            {"$match": {"vote_count": {"$gte": 100}}},

            # Extract every director and actor pair
            {"$unwind": "$crew"},
            {"$match": {"crew.job": "Director"}},
            {"$unwind": "$cast"},

            # Group by (director, actor)
            {"$group": {
                "_id": {
                    "director": "$crew.name",
                    "actor": "$cast.name"
                },
                "film_count": {"$sum": 1},
                "mean_vote": {"$avg": "$vote_average"},
                "mean_revenue": {"$avg": "$revenue"}
            }},

            # Filter only those with ≥3 collaborations
            {"$match": {"film_count": {"$gte": 3}}},

            # Sort and limit top 20
            {"$sort": {"mean_vote": -1, "film_count": -1}},
            {"$limit": 20},

            # Clean output
            {"$project": {
                "_id": 0,
                "director": "$_id.director",
                "actor": "$_id.actor",
                "film_count": 1,
                "mean_vote": {"$round": ["$mean_vote", 2]},
                "mean_revenue": {"$round": ["$mean_revenue", 0]}
            }}
        ]

        results = list(self.db.movies.aggregate(pipeline))
        print_table(
            results,
            headers=["director", "actor", "film_count", "mean_vote", "mean_revenue"]
        )
        print(f"\nReturned {len(results)} director–actor pairs.")
