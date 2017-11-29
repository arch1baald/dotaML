SELECT
    matches.match_id,
    matches.start_time,
    matches.duration,
    player_matches.account_id,
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
    player_matches.gold_t,
    player_matches.lh_t,
    player_matches.xp_t,
    player_matches.obs_log,
    player_matches.sen_log,
    player_matches.runes_log,
    player_matches.kills_log,
    player_matches.buyback_log,
    player_matches.gold_reasons,
    player_matches.xp_reasons,
    player_matches.kill_streaks,
    player_matches.multi_kills,
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
--    AND matches.start_time >= extract(epoch from timestamp '2017-06-30T21:00:00.000Z')
ORDER BY
    matches.match_id DESC NULLS LAST
    LIMIT 10000