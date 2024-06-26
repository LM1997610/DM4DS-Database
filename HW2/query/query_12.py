
import time
import pandas as pd

from tabulate import tabulate
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['soccer_db']

team1_name = 'Juventus'
team2_name =  'Atalanta'

## 12) Retrieve the historical matchup data between two specific teams, including Wins for each one and goals scored

pipeline = [

    {'$lookup': {'from': 'teams',
                 'localField': 'home_team_api_id',
                 'foreignField': 'team_api_id',
                 'as': 'home_team'  }   },

    {'$lookup': {'from': 'teams',
                 'localField': 'away_team_api_id',
                 'foreignField': 'team_api_id',
                 'as': 'away_team'} },

    {'$unwind': '$home_team'},
    {'$unwind': '$away_team'},

    {'$match': {'$or': [    {'home_team.team_long_name': team1_name, 'away_team.team_long_name': team2_name},
                            {'home_team.team_long_name': team2_name, 'away_team.team_long_name': team1_name}    ]   }   },

    {'$group': {'_id': {'team1': { '$cond': {'if': {'$eq': ['$home_team.team_long_name', team1_name]},
                                            'then': '$home_team.team_long_name',
                                            'else': '$away_team.team_long_name' }   },

                        'team2': {'$cond': {'if': {'$eq': ['$home_team.team_long_name', team1_name]},
                                            'then': '$away_team.team_long_name',
                                            'else': '$home_team.team_long_name' }   }   },

                'num_matches': {'$sum': 1},

                'team1_wins': {'$sum': {'$cond': [  {'$or': [
                              {'$and': [{'$eq': ['$home_team.team_long_name', team1_name]}, {'$gt': ['$home_team_goal', '$away_team_goal']}]},
                              {'$and': [{'$eq': ['$away_team.team_long_name', team1_name]}, {'$gt': ['$away_team_goal', '$home_team_goal']}]} ]},
                                                1,  0   ]   }   },

                'team2_wins': {'$sum': {'$cond': [{'$or': [
                              {'$and': [{'$eq': ['$home_team.team_long_name', team2_name]}, {'$gt': ['$home_team_goal', '$away_team_goal']}]},
                              {'$and': [{'$eq': ['$away_team.team_long_name', team2_name]}, {'$gt': ['$away_team_goal', '$home_team_goal']}]} ]},
                                                1,  0   ]   }   },

                'draws': {'$sum': {'$cond': [{'$eq': ['$home_team_goal', '$away_team_goal']},   1,  0]  }   },

                'team1_total_goals': {'$sum': {'$cond': [{'$eq': ['$home_team.team_long_name', team1_name]},
                                                         '$home_team_goal',
                                                         '$away_team_goal'   ]   }   },

                'team2_total_goals': {'$sum': {'$cond': [{'$eq': ['$home_team.team_long_name', team2_name]},
                                                         '$home_team_goal',
                                                         '$away_team_goal'  ]   }   }   }   },

    {'$project': {'_id': 0,
                  #team1_name: '$_id.team1',
                  #team2_name: '$_id.team2',
                  'num_matches': 1,
                  f'{team1_name}_wins': '$team1_wins',
                  f'{team2_name}_wins': '$team2_wins',
                  'draws': 1,
                  f'{team1_name}_tot_goals': '$team1_total_goals',
                  f'{team2_name}_tot_goals': '$team2_total_goals'}}
]

start_time = time.time()

result = list(db.matches.aggregate(pipeline))

df = pd.DataFrame.from_records(result) 

print()
print(tabulate(df, headers='keys', tablefmt="psql", numalign='center', stralign="center", showindex=False))

print(f"\n Execution time: {time.time() - start_time:.3f} seconds")