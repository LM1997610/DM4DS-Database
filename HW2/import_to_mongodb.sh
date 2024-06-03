mongoimport --db soccer_db --collection leagues --file db_converted/league.json --jsonArray
mongoimport --db soccer_db --collection countries --file db_converted/country.json --jsonArray
mongoimport --db soccer_db --collection teams --file db_converted/team.json --jsonArray
mongoimport --db soccer_db --collection players --file db_converted/player.json --jsonArray
mongoimport --db soccer_db --collection team_attributes --file db_converted/team_attributes.json --jsonArray
mongoimport --db soccer_db --collection player_attributes --file db_converted/player_attributes.json --jsonArray
mongoimport --db soccer_db --collection matches --file db_converted/match.json --jsonArray
