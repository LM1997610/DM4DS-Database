
-- 7) Find the top 10 teams who conceded the most goals during all seasons in away matches

-- psql -U postgres -d soccer_database -f"\HW1\query\query7.sql" ## 00

\timing on
SELECT
    t.team_long_name AS team_name,
    SUM(m.home_team_goal) AS conceded_goals,
    COUNT(*) AS num_matches
FROM team AS t JOIN match AS m ON t.team_api_id = m.away_team_api_id
GROUP BY t.team_long_name
ORDER BY conceded_goals DESC
LIMIT 10;