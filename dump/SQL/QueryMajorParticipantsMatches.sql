SELECT


-- Basic table
matches.match_id,
player_matches.account_id,
teams.team_id,
matches.leagueid,

FROM matches
JOIN match_patch using(match_id)
JOIN leagues using(leagueid)
JOIN player_matches using(match_id)
LEFT JOIN notable_players using(account_id)
LEFT JOIN teams using(team_id)
JOIN heroes ON player_matches.hero_id = heroes.id

JOIN
(
    SELECT
    
    DISTINCT matches.match_id
    
    FROM matches
    JOIN match_patch using(match_id)
    JOIN leagues using(leagueid)
    JOIN player_matches using(match_id)
    LEFT JOIN notable_players using(account_id)
    LEFT JOIN teams using(team_id)
    JOIN heroes ON player_matches.hero_id = heroes.id
    
    WHERE TRUE
    AND teams.team_id in
        (
          5,
    	  26,
    	  39,
    	  2163,
    	  1375614,
    	  1836806,
    	  1883502,
    	  2108395,
    	  2512249,
    	  2586976,
    	  2640025,
    	  2642171,
    	  3326875,
    	  3722973,
    	  3580606,
    	  3547682,
    	  4253054,
    	  4251435,
    	  1838315
        )
    AND matches.start_time >= 1483218000
    ORDER BY matches.match_id DESC NULLS LAST
    LIMIT 1000
) t1 ON matches.match_id = t1.match_id


WHERE TRUE
AND teams.team_id != 0
AND matches.start_time >= 1483218000
AND matches.start_time <= 1490907600
ORDER BY matches.match_id DESC NULLS LAST