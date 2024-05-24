
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']


pipeline = [

    {"$match": {"season": "2008/2009"}  },

    {"$lookup": {"from": "leagues",
                 "localField": "league_id",
                 "foreignField": "id",
                 "as": "league"}    },

    {"$unwind": "$league"},

    {"$project": {"league_name": "$league.name","players": {"$concatArrays": [
                        ["$home_player_1", "$home_player_2", "$home_player_3", "$home_player_4", 
                        "$home_player_5", "$home_player_6", "$home_player_7", "$home_player_8", 
                        "$home_player_9", "$home_player_10", "$home_player_11", "$away_player_1", 
                        "$away_player_2", "$away_player_3", "$away_player_4", "$away_player_5", 
                        "$away_player_6", "$away_player_7", "$away_player_8", "$away_player_9", 
                        "$away_player_10", "$away_player_11"]   ]   }   }   },

    {"$unwind": "$players"},

    {"$group": {"_id": {"player_id": "$players", "league_name": "$league_name"},
                "num_appearances": {"$sum": 1}}     },

    {"$match": {"num_appearances": {"$gte": 30}}    },

    {"$lookup": {"from": "players",
                 "localField": "_id.player_id",
                 "foreignField": "player_api_id",
                 "as": "player"}    },

    {"$unwind": "$player"},

    {"$project": {"_id": 0,
                  "player_api_id": "$_id.player_id",
                  "player_name": "$player.player_name",
                  "league_name": "$_id.league_name",
                  "num_appearances": 1, }     },

    {"$sort": {"num_appearances": -1}},

    {"$limit": 20}

]

start_time = time.time()

result = list(db.matches.aggregate(pipeline))


df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="github", numalign='center', stralign="center"))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")