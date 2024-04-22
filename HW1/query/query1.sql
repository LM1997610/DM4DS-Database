
-- 1) Find the number of draws, wins, and loses for each team in Serie A in a specific Season
-- Its basically the final table of the season.

\timing on
WITH Serie_A AS (
    SELECT l.id AS league_id
    FROM league AS l
    WHERE l.name = 'Italy Serie A'
)
,
draw_matches AS (
    SELECT m.home_team_api_id AS team_id, COUNT(*) AS num_draws
    FROM match AS m JOIN Serie_A ON m.league_id = Serie_A.league_id
    WHERE m.season = '2008/2009' AND m.home_team_goal = m.away_team_goal
    GROUP BY team_id
    UNION ALL
    SELECT m.away_team_api_id AS team_id, COUNT(*) AS num_draws
    FROM match AS m JOIN Serie_A ON m.league_id = Serie_A.league_id
    WHERE m.season = '2008/2009' AND m.home_team_goal = m.away_team_goal
    GROUP BY team_id
)
,
win_matches AS (
    SELECT m.home_team_api_id AS team_id, COUNT(*) AS num_wins
    FROM match AS m JOIN Serie_A ON m.league_id = Serie_A.league_id
    WHERE m.season = '2008/2009' AND m.home_team_goal > m.away_team_goal
    GROUP BY team_id
    UNION ALL
    SELECT m.away_team_api_id AS team_id, COUNT(*) AS num_wins
    FROM match AS m JOIN Serie_A ON m.league_id = Serie_A.league_id
    WHERE m.season = '2008/2009' AND m.home_team_goal < m.away_team_goal
    GROUP BY team_id
)
,
lose_matches AS (
    SELECT m.home_team_api_id AS team_id, COUNT(*) AS num_loses
    FROM match AS m JOIN Serie_A ON m.league_id = Serie_A.league_id
    WHERE m.season = '2008/2009' AND m.home_team_goal < m.away_team_goal
    GROUP BY team_id
    UNION ALL
    SELECT m.away_team_api_id AS team_id, COUNT(*) AS num_loses
    FROM match AS m JOIN Serie_A ON m.league_id = Serie_A.league_id
    WHERE m.season = '2008/2009' AND m.home_team_goal > m.away_team_goal
    GROUP BY team_id
)
,
team_results AS (
    SELECT team_id, SUM(num_draws) AS draws, 0 AS wins, 0 AS loses FROM draw_matches GROUP BY team_id
    UNION ALL
    SELECT team_id, 0 AS draws, SUM(num_wins) AS wins, 0 AS loses FROM win_matches GROUP BY team_id
    UNION ALL
    SELECT team_id, 0 AS draws, 0 AS wins, SUM(num_loses) FROM lose_matches GROUP BY team_id
)
SELECT t.team_long_name, SUM(tr.wins) AS wins, SUM(tr.draws) AS draws, SUM(tr.loses) AS loses
FROM team AS t
JOIN team_results AS tr ON t.team_api_id = tr.team_id
GROUP BY t.team_long_name
ORDER BY wins DESC;

-- psql -U postgres -d soccer_database -f"\HW1\query\query1.sql" ## 00