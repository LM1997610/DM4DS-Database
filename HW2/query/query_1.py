
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

serie_a = db.leagues.find_one({"name": "Italy Serie A"})
league_id = serie_a["id"]
season = "2008/2009"


pipeline = [
    
    {"$match": {"season": season,
                "league_id": league_id  }   },
    
    {"$facet": {"draws": [
                {"$match": {"$expr": {"$eq": ["$home_team_goal", "$away_team_goal"]}    }   },

                {"$group": {"_id": "$home_team_api_id",
                            "num_draws": {"$sum": 1}    }   },

                {"$unionWith": {"coll": "matches",
                                "pipeline": [   {"$match": {"season": season,
                                                 "league_id": league_id,
                                                 "$expr": {"$eq": ["$home_team_goal", "$away_team_goal"]}    }  },

                                                 {"$group": {"_id": "$away_team_api_id",
                                                             "num_draws": {"$sum": 1}   }   }   ]   }   },
                {"$group": {"_id": "$_id",
                            "num_draws": {"$sum": "$num_draws"} }   }   ],

                "wins": [
                {"$match": {"$expr": {"$gt": ["$home_team_goal", "$away_team_goal"]}    }   },

                {"$group": {"_id": "$home_team_api_id",
                            "num_wins": {"$sum": 1} }   },

                {"$unionWith": {"coll": "matches",
                                "pipeline": [   {"$match": {"season": season,
                                                            "league_id": league_id,
                                                            "$expr": {"$lt": ["$home_team_goal", "$away_team_goal"]}    }   },
                                                {"$group": {"_id": "$away_team_api_id",
                                                            "num_wins": {"$sum": 1} }   }   ]   }   },
                {"$group": {"_id": "$_id",
                            "num_wins": {"$sum": "$num_wins"}   }   }   ],
            
                "loses": [
                {"$match": {"$expr": {"$lt": ["$home_team_goal", "$away_team_goal"]}    }   },

                {"$group": {"_id": "$home_team_api_id",
                            "num_loses": {"$sum": 1}    }   },

                {"$unionWith": {"coll": "matches",
                                "pipeline": [   {"$match": {"season": season,
                                                            "league_id": league_id,
                                                            "$expr": {"$gt": ["$home_team_goal", "$away_team_goal"]}    }   },
                                                {"$group": {"_id": "$away_team_api_id",
                                                            "num_loses": {"$sum": 1}    }   }   ]   }   },
                {"$group": {"_id": "$_id",
                            "num_loses": {"$sum": "$num_loses"} }   }   ]   }   },
    
    {"$project": {"results": {"$setUnion": ["$draws", "$wins", "$loses"]    }   }   },
    {"$unwind": "$results"},

    {"$replaceRoot": {"newRoot": "$results"}    },
    
    {"$group": {"_id": "$_id",
                "draws": {"$sum": {"$cond": [{"$ifNull": ["$num_draws", False]}, "$num_draws", 0]}},
                "wins": {"$sum": {"$cond": [{"$ifNull": ["$num_wins", False]}, "$num_wins", 0]}},
                "loses": {"$sum": {"$cond": [{"$ifNull": ["$num_loses", False]}, "$num_loses", 0]}}     }   },
   
    {"$lookup": {"from": "teams",
                 "localField": "_id",
                 "foreignField": "team_api_id",
                 "as": "team_info"  }   },

    {"$unwind": "$team_info"},

    {"$project": {"_id": 0,
                  "team_name": "$team_info.team_long_name",
                  "wins": 1,
                  "draws": 1,
                  "loses": 1    }   },
    
    {"$sort": {"wins": -1   }   }

]


start_time = time.time()

result = list(db.matches.aggregate(pipeline))

df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="psql", numalign='center', stralign="center", showindex=False))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")