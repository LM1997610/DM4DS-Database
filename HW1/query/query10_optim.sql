
-- the following query eleminates the use of nested query on a table that is the biggest table and has so many rows

\timing on
WITH max_diff_match AS (
    SELECT m.id, m.league_id, m.season, m.home_team_api_id, m.away_team_api_id, ABS(m.B365H - m.B365A) AS odds_diff
    FROM match AS m
    ORDER BY odds_diff DESC NULLS LAST
    LIMIT 1
)
SELECT l.name AS league_id, mdm.season, t1.team_long_name AS home_team, t2.team_long_name AS away_team, mdm.odds_diff
FROM league AS l 
JOIN max_diff_match AS mdm ON l.id = mdm.league_id
JOIN team AS t1 ON t1.team_api_id = mdm.home_team_api_id
JOIN team AS t2 ON t2.team_api_id = mdm.away_team_api_id
