
-- 3) Retrive the Players who played at least 30 games in a given Season:

-- -- psql -U postgres -d soccer_database -f"\HW1\query\query3.sql" # 00

\timing on
SELECT p.player_api_id, 
       p.player_name, 
       COUNT(*) AS num_appearances, 
       l.name AS league_name

FROM Player AS p, Match as m
JOIN League l ON m.league_id = l.id

WHERE m.season = '2008/2009' AND (p.player_api_id = m.home_player_1 
                               OR p.player_api_id = m.home_player_2
                               OR p.player_api_id = m.home_player_3
                               OR p.player_api_id = m.home_player_4
                               OR p.player_api_id = m.home_player_5
                               OR p.player_api_id = m.home_player_6
                               OR p.player_api_id = m.home_player_7
                               OR p.player_api_id = m.home_player_8
                               OR p.player_api_id = m.home_player_9
                               OR p.player_api_id = m.home_player_10
                               OR p.player_api_id = m.home_player_11
                               OR p.player_api_id = m.away_player_1
                               OR p.player_api_id = m.away_player_2
                               OR p.player_api_id = m.away_player_3
                               OR p.player_api_id = m.away_player_4
                               OR p.player_api_id = m.away_player_5
                               OR p.player_api_id = m.away_player_6
                               OR p.player_api_id = m.away_player_7
                               OR p.player_api_id = m.away_player_8
                               OR p.player_api_id = m.away_player_9
                               OR p.player_api_id = m.away_player_10
                               OR p.player_api_id = m.away_player_11)

GROUP BY (player_api_id, p.player_name, l.name)
HAVING (COUNT(*) >= 30)
ORDER BY num_appearances DESC
LIMIT 20;
