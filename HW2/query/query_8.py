
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']


pipeline = [

    {"$lookup": {"from": "matches",
                 "localField": "id",
                 "foreignField": "league_id",
                 "as": "all_matches"}},

    {"$unwind": "$all_matches"},

    {"$match": {"all_matches.season": "2008/2009"}},

    {"$group": {"_id": "$id",
                "max_date": {"$max": "$all_matches.date"},
                "league_name": {"$first": "$name"}}},

    {"$sort": {"max_date": 1}},

    {"$limit": 1},

    {"$project": {"last_match_date": "$max_date",
                  "_id": 0,
                  "league_id": "$_id",
                  "league_name": 1,}}         

]


start_time = time.time()

result = list(db.leagues.aggregate(pipeline))


df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="github", numalign='center', stralign="center"))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")