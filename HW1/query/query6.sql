
-- 6) Find the top 10 teams who gained the most points during all seasons:

\timing on
WITH draw_matches AS (
    SELECT m.home_team_api_id AS team_id, COUNT(*) AS num_draws
    FROM match AS m
    WHERE m.home_team_goal = m.away_team_goal
    GROUP BY team_id
    UNION ALL
    SELECT m.away_team_api_id AS team_id, COUNT(*) AS num_draws
    FROM match AS m
    WHERE m.home_team_goal = m.away_team_goal
    GROUP BY team_id
)
,
win_matches AS (
    SELECT m.home_team_api_id AS team_id, COUNT(*) AS num_wins
    FROM match AS m
    WHERE m.home_team_goal > m.away_team_goal
    GROUP BY team_id
    UNION ALL
    SELECT m.away_team_api_id AS team_id, COUNT(*) AS num_wins
    FROM match AS m
    WHERE m.home_team_goal < m.away_team_goal
    GROUP BY team_id
)
,
lose_matches AS (
    SELECT m.home_team_api_id AS team_id, COUNT(*) AS num_loses
    FROM match AS m
    WHERE m.home_team_goal < m.away_team_goal
    GROUP BY team_id
    UNION ALL
    SELECT m.away_team_api_id AS team_id, COUNT(*) AS num_loses
    FROM match AS m
    WHERE m.home_team_goal > m.away_team_goal
    GROUP BY team_id
)
,
team_results AS (
    SELECT team_id, SUM(num_draws) AS draws, 0 AS wins, 0 AS loses FROM draw_matches GROUP BY team_id
    UNION ALL
    SELECT team_id, 0 AS draws, SUM(num_wins) AS wins, 0 AS loses FROM win_matches GROUP BY team_id
    UNION ALL
    SELECT team_id, 0 AS draws, 0 AS wins, SUM(num_loses) FROM lose_matches GROUP BY team_id
),
team_wdl AS (
    SELECT t.team_long_name, SUM(tr.wins) AS wins, SUM(tr.draws) AS draws, SUM(tr.loses) AS loses
    FROM team AS t
    JOIN team_results AS tr ON t.team_api_id = tr.team_id
    GROUP BY t.team_long_name
)
SELECT twdl.team_long_name, (twdl.wins * 3 + twdl.draws) AS points
FROM team_wdl AS twdl
ORDER BY points DESC
LIMIT 10;

-- psql -U postgres -d soccer_database -f"\HW1\query\query6.sql" ## 00