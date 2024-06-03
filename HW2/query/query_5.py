
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

## 5) Show average statistics for specific players

pipeline = [

    {"$match": {"$or": [{"player_name": "Lionel Messi"},
                        {"player_name": "Cristiano Ronaldo"}]}},

    {"$lookup": {"from": "player_attributes",
                 "localField": "player_api_id",
                 "foreignField": "player_api_id",
                 "as": "attributes"}}, 

    {"$unwind": "$attributes"}, 

    {"$group": {"_id": "$player_name",
                "avg_rating": {"$avg": "$attributes.overall_rating"},
                "avg_potential": {"$avg": "$attributes.potential"},
                "avg_crossing": {"$avg": "$attributes.crossing"},
                "avg_finishing": {"$avg": "$attributes.finishing"}}}    
                
    ]


start_time = time.time()

result = list(db.players.aggregate(pipeline))


df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="psql", numalign='center', stralign="center", showindex=False))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")

