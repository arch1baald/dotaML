SELECT


-- Basic table
matches.match_id,
player_matches.account_id,
teams.team_id,
matches.leagueid,
leagues.name leaguename,
matches.start_time,
player_matches.hero_id,
player_matches.player_slot,
((player_matches.player_slot < 128) = matches.radiant_win) win,
((player_matches.player_slot < 128) = true) radiant, --new
((player_matches.player_slot < 128) = false) dire, --new
player_matches.kills,
player_matches.deaths,
player_matches.assists,
player_matches.gold_per_min ,
player_matches.xp_per_min ,
player_matches.gold_spent ,
player_matches.hero_damage ,
player_matches.tower_damage ,
player_matches.stuns ,
player_matches.creeps_stacked ,
player_matches.camps_stacked ,
player_matches.hero_healing ,
player_matches.last_hits,
player_matches.denies,
player_matches.level,
matches.duration,
matches.cluster,
matches.first_blood_time,

--timeStamp variables
player_matches.gold_t[3] t_Gold_cnt_3,
player_matches.gold_t[5] t_Gold_cnt_5,
player_matches.gold_t[8] t_Gold_cnt_8,
player_matches.gold_t[10] t_Gold_cnt_10,
player_matches.gold_t[12] t_Gold_cnt_12,
player_matches.gold_t[15] t_Gold_cnt_15,
player_matches.gold_t[20] t_Gold_cnt_20,
player_matches.gold_t[25] t_Gold_cnt_25,
player_matches.gold_t[30] t_Gold_cnt_30,

player_matches.lh_t[3] t_LastHits_cnt_3,
player_matches.lh_t[5] t_LastHits_cnt_5,
player_matches.lh_t[8] t_LastHits_cnt_8,
player_matches.lh_t[10] t_LastHits_cnt_10,
player_matches.lh_t[12] t_LastHits_cnt_12,
player_matches.lh_t[15] t_LastHits_cnt_15,
player_matches.lh_t[20] t_LastHits_cnt_20,
player_matches.lh_t[25] t_LastHits_cnt_25,
player_matches.lh_t[30] t_LastHits_cnt_30,

player_matches.xp_t[3] t_Experience_cnt_3,
player_matches.xp_t[5] t_Experience_cnt_5,
player_matches.xp_t[8] t_Experience_cnt_8,
player_matches.xp_t[10] t_Experience_cnt_10,
player_matches.xp_t[12] t_Experience_cnt_12,
player_matches.xp_t[15] t_Experience_cnt_15,
player_matches.xp_t[20] t_Experience_cnt_20,
player_matches.xp_t[25] t_Experience_cnt_25,
player_matches.xp_t[30] t_Experience_cnt_30,

player_matches.pings ping_log,
player_matches.obs_log,
player_matches.sen_log,
player_matches.runes_log,
player_matches.kills_log,
player_matches.buyback_log,

--table_heroStats
heroes.localized_name,
heroes.attack_type,
heroes.primary_attr,
heroes.roles role_log,

--purchase_log
player_matches.purchase_log

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
ORDER BY matches.match_id DESC NULLS LAST