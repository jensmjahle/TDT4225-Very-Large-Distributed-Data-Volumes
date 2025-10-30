from print_helper import print_table

class Task9:
    def __init__(self, db):
        self.db = db

    def run(self):
        print("\n--- TASK 9: Top 10 Original Languages (non-English with U.S. involvement) ---")

        pipeline = [
            # 1️⃣ Filter: non-English + has U.S. production country
            {"$match": {
                "original_language": {"$ne": "en"},
                "production_countries.iso_3166_1": "US"
            }},

            # 2️⃣ Group by original language, collect example title
            {"$group": {
                "_id": "$original_language",
                "count": {"$sum": 1},
                "example_title": {"$first": "$title"}
            }},

            # 3️⃣ Sort by count descending, then take top 10
            {"$sort": {"count": -1}},
            {"$limit": 10},

            # 4️⃣ Final projection
            {"$project": {
                "_id": 0,
                "language": "$_id",
                "count": 1,
                "example_title": 1
            }}
        ]

        results = list(self.db.movies.aggregate(pipeline))
        print_table(results, headers=["language", "count", "example_title"])
        print(f"\nReturned {len(results)} languages.")
