
--11) This query is made to find the average number of goals per game for each league in each season and returns the top-3

-- psql -U postgres -d soccer_database -f"\HW1\query\query11.sql" ## 00

\timing on

SELECT l.name, 
       m.season, 
       AVG(m.away_team_goal + m.home_team_goal) AS avg_goal_per_game

FROM league AS l 
JOIN match AS m ON l.id = m.league_id

GROUP BY (l.name, m.season)
ORDER BY avg_goal_per_game DESC

LIMIT 3