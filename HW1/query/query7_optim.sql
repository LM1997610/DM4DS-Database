
-- create index on match.away_team_api_id and match.home_team_goal reduce the number of joins 
-- boost: over 50 %
-- the sort cost is triple the time in unoptimized query.

-- command for creating the index:
-- psql -U mosix11 -h localhost -d soccer -c "CREATE INDEX IF NOT EXISTS match_away_index ON match(away_team_api_id, home_team_goal);"
-- or uncomment the following line
-- CREATE INDEX IF NOT EXISTS match_away_index ON match(away_team_api_id, home_team_goal);

-- command for dropping index
-- psql -U mosix11 -h localhost -d soccer -c "DROP INDEX IF EXISTS match_away_index;"

\timing on
WITH conceded_goals AS (
    SELECT m.away_team_api_id AS team_id, SUM(m.home_team_goal) AS c_goals, COUNT(*) AS num_match
    FROM match AS m
    GROUP BY team_id
)
SELECT t.team_long_name AS team_name, cg.c_goals, cg.num_match
FROM team AS t 
JOIN conceded_goals AS cg ON t.team_api_id = cg.team_id 
ORDER BY cg.c_goals DESC
LIMIT 10;
