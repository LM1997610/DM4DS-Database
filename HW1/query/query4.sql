-- 4) Find tallest players in each league in a specific Season

-- psql -U postgres -d soccer_database -f"\HW1\query\query4.sql" ## 00

\timing on
WITH league_season_players AS (
    SELECT DISTINCT l.name AS league_name, p.player_api_id as player_id, p.player_name AS player_name, p.height AS player_height
    FROM player AS p JOIN match as m ON p.player_api_id IN (m.home_player_1,
                                                            m.home_player_2,
                                                            m.home_player_3,
                                                            m.home_player_4,
                                                            m.home_player_5,
                                                            m.home_player_6,
                                                            m.home_player_7,
                                                            m.home_player_8,
                                                            m.home_player_9,
                                                            m.home_player_10,
                                                            m.home_player_11,
                                                            m.away_player_1,
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
    JOIN league AS l ON m.league_id = l.id
    WHERE m.season = '2008/2009'
)
SELECT lsp.league_name, lsp.player_name, lsp.player_height
FROM league_season_players AS lsp
WHERE lsp.player_height >= all (SELECT player_height 
                                FROM league_season_players AS ilsp 
                                WHERE lsp.league_name = ilsp.league_name)
ORDER BY league_name;