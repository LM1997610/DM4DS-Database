
--10) Find the match with biggest difference in Home and Away odds.

\timing on
SELECT l.name AS league_id, m.season, t1.team_long_name AS home_team, t2.team_long_name AS away_team, ABS(m.B365H - m.B365A) AS odds_diff
FROM league AS l
JOIN match AS m ON l.id = m.league_id
JOIN team AS t1 ON t1.team_api_id = m.home_team_api_id
JOIN team AS t2 ON t2.team_api_id = m.away_team_api_id
WHERE ABS(COALESCE(m.B365H, 0) - COALESCE(m.B365A, 0)) >= all (SELECT ABS(COALESCE(B365H, 0) - COALESCE(B365A, 0)) FROM match)

