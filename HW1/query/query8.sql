
--8) Which league ended earlier in one specific season:

--EXPLAIN ANALYZE

\timing on

SELECT l.name AS league_name, 
       MAX(m.date) AS last_match_date

FROM league AS l 
JOIN match AS m ON l.id = m.league_id

WHERE m.season = '2008/2009'
GROUP BY l.name
ORDER BY last_match_date ASC
LIMIT 1;
