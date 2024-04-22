
-- 2) Find the Champion of each league in a specific Season

\timing on
WITH draw_matches AS (
    SELECT l.name AS league_name, m.home_team_api_id AS team_id, COUNT(*) AS num_draws
    FROM match AS m JOIN league AS l ON m.league_id = l.id
    WHERE m.season = '2009/2010' AND m.home_team_goal = m.away_team_goal
    GROUP BY (league_name, team_id)
    UNION ALL
    SELECT l.name AS league_name, m.away_team_api_id AS team_id, COUNT(*) AS num_draws
    FROM match AS m JOIN league AS l ON m.league_id = l.id
    WHERE m.season = '2009/2010' AND m.home_team_goal = m.away_team_goal
    GROUP BY (league_name, team_id)
)
,
win_matches AS (
    SELECT l.name AS league_name, m.home_team_api_id AS team_id, COUNT(*) AS num_wins
    FROM match AS m JOIN league AS l ON m.league_id = l.id
    WHERE m.season = '2009/2010' AND m.home_team_goal > m.away_team_goal
    GROUP BY (league_name, team_id)
    UNION ALL
    SELECT l.name AS league_name, m.away_team_api_id AS team_id, COUNT(*) AS num_wins
    FROM match AS m JOIN league AS l ON m.league_id = l.id
    WHERE m.season = '2009/2010' AND m.home_team_goal < m.away_team_goal
    GROUP BY (league_name, team_id)
)
,
lose_matches AS (
    SELECT l.name AS league_name, m.home_team_api_id AS team_id, COUNT(*) AS num_loses
    FROM match AS m JOIN league AS l ON m.league_id = l.id
    WHERE m.season = '2009/2010' AND m.home_team_goal < m.away_team_goal
    GROUP BY (league_name, team_id)
    UNION ALL
    SELECT l.name AS league_name, m.away_team_api_id AS team_id, COUNT(*) AS num_loses
    FROM match AS m JOIN league AS l ON m.league_id = l.id
    WHERE m.season = '2009/2010' AND m.home_team_goal > m.away_team_goal
    GROUP BY (league_name, team_id)
)
,
team_results AS (
    SELECT league_name, team_id, SUM(num_draws) AS draws, 0 AS wins, 0 AS loses FROM draw_matches GROUP BY (league_name, team_id)
    UNION ALL
    SELECT league_name, team_id, 0 AS draws, SUM(num_wins) AS wins, 0 AS loses FROM win_matches GROUP BY (league_name, team_id)
    UNION ALL
    SELECT league_name, team_id, 0 AS draws, 0 AS wins, SUM(num_loses) FROM lose_matches GROUP BY (league_name, team_id)
),
team_wdl AS (
    SELECT tr.league_name, t.team_long_name, SUM(tr.wins) AS wins, SUM(tr.draws) AS draws, SUM(tr.loses) AS loses
    FROM team AS t
    JOIN team_results AS tr ON t.team_api_id = tr.team_id
    GROUP BY (tr.league_name, t.team_long_name)
)
SELECT twdl.league_name, twdl.team_long_name, (twdl.wins * 3 + twdl.draws) AS points
FROM team_wdl AS twdl
WHERE (twdl.wins * 3 + twdl.draws) >= all (SELECT (wins * 3 + draws) AS p
                     FROM team_wdl
                     WHERE league_name = twdl.league_name);