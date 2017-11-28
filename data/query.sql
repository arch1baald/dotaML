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
    player_matches.rune_pickups,
    player_matches.lane,
    player_matches.lane_role,
    player_matches.is_roaming,
    player_matches.teamfight_participation,
    player_matches.towers_killed,
    player_matches.roshans_killed,
    player_matches.observers_placed,
    matches.duration,
    matches.first_blood_time,

    --timeStamp variables
    player_matches.gold_t[3] t_gold_cnt_3,
    player_matches.gold_t[5] t_gold_cnt_5,
    player_matches.gold_t[8] t_gold_cnt_8,
    player_matches.gold_t[10] t_gold_cnt_10,
    player_matches.gold_t[12] t_gold_cnt_12,
    player_matches.gold_t[15] t_gold_cnt_15,
    player_matches.gold_t[20] t_gold_cnt_20,
    player_matches.gold_t[25] t_gold_cnt_25,
    player_matches.gold_t[30] t_gold_cnt_30,

    player_matches.lh_t[3] t_lasthits_cnt_3,
    player_matches.lh_t[5] t_lasthits_cnt_5,
    player_matches.lh_t[8] t_lasthits_cnt_8,
    player_matches.lh_t[10] t_lasthits_cnt_10,
    player_matches.lh_t[12] t_lasthits_cnt_12,
    player_matches.lh_t[15] t_lasthits_cnt_15,
    player_matches.lh_t[20] t_lasthits_cnt_20,
    player_matches.lh_t[25] t_lasthits_cnt_25,
    player_matches.lh_t[30] t_lasthits_cnt_30,

    player_matches.xp_t[3] t_experience_cnt_3,
    player_matches.xp_t[5] t_experience_cnt_5,
    player_matches.xp_t[8] t_experience_cnt_8,
    player_matches.xp_t[10] t_experience_cnt_10,
    player_matches.xp_t[12] t_experience_cnt_12,
    player_matches.xp_t[15] t_experience_cnt_15,
    player_matches.xp_t[20] t_experience_cnt_20,
    player_matches.xp_t[25] t_experience_cnt_25,
    player_matches.xp_t[30] t_experience_cnt_30,

    player_matches.pings ping_log,
    player_matches.obs_log,
    player_matches.sen_log,
    player_matches.runes_log,
    player_matches.kills_log,
    player_matches.buyback_log,
    
    player_matches.gold_reasons,
    player_matches.xp_reasons,
    player_matches.kill_streaks,
    player_matches.multi_kills,


    --table_heroStats
    heroes.localized_name,
    heroes.attack_type,
    heroes.primary_attr,
    heroes.roles role_log,

    --purchase_log
    player_matches.purchase_log
FROM 
    matches
        JOIN match_patch using(match_id)
        JOIN leagues using(leagueid)
        JOIN player_matches using(match_id)
        JOIN heroes on heroes.id = player_matches.hero_id
        LEFT JOIN notable_players ON notable_players.account_id = player_matches.account_id AND 
            notable_players.locked_until = (SELECT MAX(locked_until) FROM notable_players)
        LEFT JOIN teams using(team_id)
WHERE TRUE
    AND matches.leagueid = 5401 OR matches.leagueid = 4442
    AND matches.start_time >= extract(epoch from timestamp '2017-06-30T21:00:00.000Z')
    AND teams.team_id IN (5, 15, 39, 46, 2163, 350190, 1375614, 1838315,
        1883502, 2108395, 2512249, 2581813, 2586976, 2640025, 2672298, 1333179, 3331948, 1846548)
ORDER BY 
    matches.match_id DESC NULLS LAST
    LIMIT 10000