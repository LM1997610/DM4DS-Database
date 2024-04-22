
-- using case conditioning in the SELECT clause instead of using union

\timing on
WITH team_performance AS (
    SELECT 
        l.name AS league_name, 
        CASE WHEN m.home_team_goal > m.away_team_goal THEN m.home_team_api_id
             WHEN m.home_team_goal < m.away_team_goal THEN m.away_team_api_id
        END AS winner,
        CASE WHEN m.home_team_goal < m.away_team_goal THEN m.home_team_api_id
             WHEN m.home_team_goal > m.away_team_goal THEN m.away_team_api_id
        END AS loser,
        CASE WHEN m.home_team_goal = m.away_team_goal THEN m.home_team_api_id ELSE NULL END AS draw_home,
        CASE WHEN m.home_team_goal = m.away_team_goal THEN m.away_team_api_id ELSE NULL END AS draw_away,
        m.season
    FROM 
        match m
        JOIN league l ON m.league_id = l.id
    WHERE 
        m.season = '2009/2010'
),
aggregated_points AS (
    SELECT 
        league_name, 
        team_id, 
        SUM(points) AS total_points
    FROM (
        SELECT league_name, winner AS team_id, 3 AS points, season FROM team_performance WHERE winner IS NOT NULL
        UNION ALL
        SELECT league_name, loser AS team_id, 0 AS points, season FROM team_performance WHERE loser IS NOT NULL
        UNION ALL
        SELECT league_name, draw_home AS team_id, 1 AS points, season FROM team_performance WHERE draw_home IS NOT NULL
        UNION ALL
        SELECT league_name, draw_away AS team_id, 1 AS points, season FROM team_performance WHERE draw_away IS NOT NULL
    ) AS points_data
    WHERE team_id IS NOT NULL
    GROUP BY league_name, team_id
),
ranked_teams AS (
    SELECT 
        ap.league_name, 
        ap.team_id, 
        ap.total_points
    FROM aggregated_points AS ap
    WHERE total_points >= all (SELECT total_points FROM aggregated_points WHERE ap.league_name = league_name)
)
SELECT rt.league_name, t.team_long_name, rt.total_points 
FROM ranked_teams AS rt
JOIN team AS t ON t.team_api_id = rt.team_id;
-- SELECT * FROM aggregated_points;

