
-- first by adding distinct to the palyer_teams CTE the execution time decreses! the reason is early deduplication
-- there is a sort that takes roughly triple the time in the slower query compared to the faster query

\timing on
WITH player_teams AS (
    SELECT DISTINCT p.player_api_id AS player_id, p.player_name, t.team_api_id AS team_id, t.team_long_name AS team_name
    FROM player AS p JOIN match AS m ON p.player_api_id IN (m.home_player_1,
                                                            m.home_player_2,
                                                            m.home_player_3,
                                                            m.home_player_4,
                                                            m.home_player_5,
                                                            m.home_player_6,
                                                            m.home_player_7,
                                                            m.home_player_8,
                                                            m.home_player_9,
                                                            m.home_player_10,
                                                            m.home_player_11)
    JOIN team AS t ON t.team_api_id = m.home_team_api_id
    UNION
    SELECT DISTINCT p.player_api_id AS player_id, p.player_name, t.team_api_id AS team_id, t.team_long_name AS team_name
    FROM player AS p JOIN match AS m ON p.player_api_id IN (m.away_player_1,
                                                            m.away_player_2,
                                                            m.away_player_3,
                                                            m.away_player_4,
                                                            m.away_player_5,
                                                            m.away_player_6,
                                                            m.away_player_7,
                                                            m.away_player_8,
                                                            m.away_player_9,
                                                            m.away_player_10,
                                                            m.away_player_11)
    JOIN team AS t ON t.team_api_id = m.away_team_api_id
)
SELECT player_id, player_name, COUNT(team_name) AS num_teams
FROM player_teams
GROUP BY (player_id, player_name)
ORDER BY num_teams DESC
LIMIT 10;

-- ##################################################################################################################################

-- second by creating materialized view of the players_teams the query execution time deacreases drastically.

-- CREATE MATERIALIZED VIEW player_teams AS
-- SELECT p.player_api_id AS player_id, p.player_name, t.team_api_id AS team_id, t.team_long_name AS team_name
-- FROM player AS p JOIN match AS m ON p.player_api_id IN (m.home_player_1,
--                                                         m.home_player_2,
--                                                         m.home_player_3,
--                                                         m.home_player_4,
--                                                         m.home_player_5,
--                                                         m.home_player_6,
--                                                         m.home_player_7,
--                                                         m.home_player_8,
--                                                         m.home_player_9,
--                                                         m.home_player_10,
--                                                         m.home_player_11)
-- JOIN team AS t ON t.team_api_id = m.home_team_api_id
-- UNION
-- SELECT p.player_api_id AS player_id, p.player_name, t.team_api_id AS team_id, t.team_long_name AS team_name
-- FROM player AS p JOIN match AS m ON p.player_api_id IN (m.away_player_1,
--                                                         m.away_player_2,
--                                                         m.away_player_3,
--                                                         m.away_player_4,
--                                                         m.away_player_5,
--                                                         m.away_player_6,
--                                                         m.away_player_7,
--                                                         m.away_player_8,
--                                                         m.away_player_9,
--                                                         m.away_player_10,
--                                                         m.away_player_11)
-- JOIN team AS t ON t.team_api_id = m.away_team_api_id;

-- -- EXPLAIN ANALYZE
-- SELECT player_id, player_name, COUNT(team_name) AS num_teams
-- FROM player_teams
-- GROUP BY (player_id, player_name)
-- ORDER BY num_teams DESC
-- LIMIT 10;

-- DROP MATERIALIZED VIEW player_teams


-- ##################################################################################################################################
