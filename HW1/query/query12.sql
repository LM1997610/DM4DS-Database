
--12) Retrieve the historical matchup data between two specific teams, including win-loss record and total goals scored.

\timing on
SELECT
    t1.team_long_name AS team1,
    t2.team_long_name AS team2,
    COUNT(*) AS num_matches,
    SUM(
        CASE 
            WHEN (m.home_team_api_id = t1.team_api_id AND m.home_team_goal > m.away_team_goal)
                  OR (m.away_team_api_id = t1.team_api_id AND m.home_team_goal < m.away_team_goal)
                  THEN 1 ELSE 0 END

    ) AS team1_wins,
    SUM(
        CASE
            WHEN (m.home_team_goal = m.away_team_goal) THEN 1 ELSE 0 END
    ) AS draws,
    SUM(
        CASE 
            WHEN (m.home_team_api_id = t2.team_api_id AND m.home_team_goal > m.away_team_goal)
                  OR (m.away_team_api_id = t2.team_api_id AND m.home_team_goal < m.away_team_goal)
                  THEN 1 ELSE 0 END

    ) AS team2_wins,
    SUM(
        CASE
            WHEN (m.home_team_api_id = t1.team_api_id) THEN m.home_team_goal
            ELSE m.away_team_goal END
    ) AS team1_total_goals,
    SUM(
        CASE
            WHEN (m.home_team_api_id = t2.team_api_id) THEN m.home_team_goal
            ELSE m.away_team_goal END
    ) AS team2_total_goals
FROM
    team AS t1 JOIN match AS m ON t1.team_api_id IN (m.home_team_api_id, m.away_team_api_id)
    JOIN team AS t2 ON t2.team_api_id IN (m.home_team_api_id, m.away_team_api_id)
    WHERE t1.team_long_name = 'Real Madrid CF' AND t2.team_long_name = 'Villarreal CF' 
GROUP BY (t1.team_long_name, t2.team_long_name)

--FC Barcelona
--psql -U postgres -d soccer_database -f"\HW1\query\query12.sql" ## 00