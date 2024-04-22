
-- 3) Retrive the Players who played at least 30 games in a given Season:

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

-- WITH player_appearances AS (SELECT home_player_1 AS player_api_id FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_2 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_3 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_4 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_5 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_6 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_7 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_8 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_9 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_10 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT home_player_11 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_1 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_2 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_3 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_4 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_5 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_6 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_7 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_8 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_9 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_10 FROM match WHERE season = '2008/2009'
--                   UNION ALL SELECT away_player_11 FROM match WHERE season = '2008/2009')
--     
-- SELECT p.player_api_id, p.player_name, COUNT(*) AS num_appearances
-- FROM player AS p
-- JOIN player_appearances AS pa ON p.player_api_id = pa.player_api_id
-- GROUP BY (p.player_api_id, p.player_name)
-- HAVING (COUNT(*) >= 30)
-- ORDER BY num_appearances DESC
-- LIMIT 20