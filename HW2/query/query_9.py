import time
import pandas as pd

from collections import defaultdict
from tabulate import tabulate

from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

def create_player_lookups(position_prefix):

    result = [  {"$lookup": {"from": "players",
                             "localField": f"{position_prefix}_player_{i}",
                             "foreignField": "player_api_id",
                             "as": f"player_{i}_info"   }   } for i in range(1, 12)     ]
    return result

def create_player_projections(position_prefix):

    result =  [ {"$project": {  "match_id": 1,
                               f"{position_prefix}_team_api_id": 1,
                               "player_info": { "$concatArrays": [f"$player_{i}_info" for i in range(1, 12)]    }   }  },
                {"$unwind": "$player_info"},

                {"$project": {  "player_id": "$player_info.player_api_id",
                                "player_name": "$player_info.player_name",
                                "team_id": f"${position_prefix}_team_api_id"    }   }   ]
    return result


start_time = time.time()   


home_pipeline = [   {"$match": {"home_player_1": {"$exists": True}}},
                 *create_player_lookups("home"),
                 *create_player_projections("home")     ]

away_pipeline = [   {"$match": {"away_player_1": {"$exists": True}}},
                *create_player_lookups("away"),
                *create_player_projections("away")      ]

home_results = db.matches.aggregate(home_pipeline)

away_results = db.matches.aggregate(away_pipeline)

combined_results = list(home_results) + list(away_results)


player_team_counts = defaultdict(set)

for entry in combined_results:
    player_team_counts[(entry["player_id"], entry["player_name"])].add(entry["team_id"])


player_team_counts = [{"player_id": key[0], "player_name": key[1], "num_teams": len(value)} for key, value in player_team_counts.items()]
player_team_counts.sort(key=lambda x: x["num_teams"], reverse=True)


top_10_players = player_team_counts[:10]

df = pd.DataFrame.from_records(top_10_players) 

print()
print(tabulate(df, headers='keys', tablefmt="github", numalign='center', stralign="center"))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")