SELECT
matches.match_id,
player_matches.account_id,
notable_players.team_id,
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
matches.duration,
matches.cluster,
matches.first_blood_time,
heroes.localized_name,
heroes.attack_type,
heroes.primary_attr
FROM matches
JOIN match_patch using(match_id)
JOIN leagues using(leagueid)
JOIN player_matches using(match_id)
LEFT JOIN notable_players using(account_id)
--LEFT JOIN teams using(team_id)
JOIN heroes ON player_matches.hero_id = heroes.id
WHERE TRUE
AND notable_players.team_id IN
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