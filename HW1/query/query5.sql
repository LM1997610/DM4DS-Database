
-- 5) Show average statistics for specific players (Messi and Ronaldo)

-- -- psql -U postgres -d soccer_database -f"\HW1\query\query5.sql" ## 00

SELECT p.player_name, 
       AVG(pl.overall_rating) as avg_rating,
       AVG(pl.potential) as avg_potential,
       AVG(pl.crossing) as avg_crossing,
       AVG(pl.finishing) as avg_finishing

FROM player AS p 

JOIN player_attributes as pl ON p.player_api_id = pl.player_api_id

WHERE p.player_name LIKE 'Lionel Messi' OR p.player_name LIKE 'Cristiano Ronaldo'

GROUP BY p.player_name
