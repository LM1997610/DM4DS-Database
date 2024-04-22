
-- create materialized view for league season players improved performane by 50 %


CREATE MATERIALIZED VIEW IF NOT EXISTS league_season_players AS (
    SELECT DISTINCT l.id AS league_id, l.name AS league_name, mp.season AS season,  mp.player_api_id
    FROM (
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_1 AS player_api_id FROM match AS m WHERE m.home_player_1 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_2 AS player_api_id FROM match AS m WHERE m.home_player_2 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_3 AS player_api_id FROM match AS m WHERE m.home_player_3 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_4 AS player_api_id FROM match AS m WHERE m.home_player_4 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_5 AS player_api_id FROM match AS m WHERE m.home_player_5 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_6 AS player_api_id FROM match AS m WHERE m.home_player_6 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_7 AS player_api_id FROM match AS m WHERE m.home_player_7 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_8 AS player_api_id FROM match AS m WHERE m.home_player_8 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_9 AS player_api_id FROM match AS m WHERE m.home_player_9 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_10 AS player_api_id FROM match AS m WHERE m.home_player_10 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.home_player_11 AS player_api_id FROM match AS m WHERE m.home_player_11 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_1 AS player_api_id FROM match AS m WHERE m.away_player_1 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_2 AS player_api_id FROM match AS m WHERE m.away_player_2 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_3 AS player_api_id FROM match AS m WHERE m.away_player_3 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_4 AS player_api_id FROM match AS m WHERE m.away_player_4 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_5 AS player_api_id FROM match AS m WHERE m.away_player_5 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_6 AS player_api_id FROM match AS m WHERE m.away_player_6 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_7 AS player_api_id FROM match AS m WHERE m.away_player_7 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_8 AS player_api_id FROM match AS m WHERE m.away_player_8 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_9 AS player_api_id FROM match AS m WHERE m.away_player_9 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_10 AS player_api_id FROM match AS m WHERE m.away_player_10 IS NOT NULL
        UNION ALL
        SELECT m.id as match_id, m.season AS season, m.league_id AS league_id, m.away_player_11 AS player_api_id FROM match AS m WHERE m.away_player_11 IS NOT NULL
    ) AS mp JOIN league AS l ON l.id = mp.league_id
);
\timing on
WITH m_query AS (
    SELECT lsp.league_name, p.player_name, p.height
    FROM league_season_players AS lsp JOIN player AS p ON lsp.player_api_id = p.player_api_id
    WHERE lsp.season = '2008/2009'
)
SELECT mq.league_name, mq.player_name, mq.height
FROM m_query AS mq
WHERE mq.height >= ALL (SELECT height FROM m_query WHERE mq.league_name = league_name)
ORDER BY league_name;
