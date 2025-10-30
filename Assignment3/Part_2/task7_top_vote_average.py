from print_helper import print_table

class Task7:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 7: Top 20 'Noir' or 'Neo-Noir' Movies ---")

        pipeline = [
            # Text search over overview & tagline
            {"$match": {
                "$text": {"$search": "noir OR neo-noir"},
                "vote_count": {"$gte": 50}
            }},

            # Sort by vote_average descending
            {"$sort": {"vote_average": -1, "vote_count": -1}},

            # Return the 20 best results
            {"$limit": 20},

            # Only keep required fields
            {"$project": {
                "_id": 0,
                "title": 1,
                "year": 1,
                "vote_average": 1,
                "vote_count": 1
            }}
        ]

        results = list(self.db.movies.aggregate(pipeline))
        print_table(results, headers=["title", "year", "vote_average", "vote_count"])
        print(f"\nReturned {len(results)} noir/neo-noir movies.")
