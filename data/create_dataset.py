import sys
import io
import datetime

import requests
import numpy as np
import pandas as pd


QUERY = '''
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
    matches.duration,
    matches.cluster,
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
'''


def download_google_spreadsheet(url):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        content_bytes = resp.content
    except requests.exceptions.HTTPError as err:
        print(err, file=sys.stderr)
    try:
        content = pd.read_csv(io.BytesIO(content_bytes))
        return content
    except Exception as err:
        print("failed to convert bytes to csv:", err, file=sys.stderr)


def query_opendota(sql):
    resp = requests.get('https://api.opendota.com/api/explorer', params={'sql': sql})
    data = resp.json()
    if resp.status_code == 400 and data is not None:
        sys.stderr.write(data['err'])
    resp.raise_for_status()
    return pd.DataFrame.from_records(data['rows'])


def create_unique_id(df_matches):
    unique_ids = []
    for _, row in df_matches.iterrows():
        unique_id = '{}_{}'.format(row['match_id'], row['account_id'])
        unique_ids.append(unique_id)
    return unique_ids


def create_core_items_timings(df_matches):
    t_item = dict()
    core_items = [
        'blink',
        'power_treads',
        'ultimate_scepter',
        'phase_boots',
        'travel_boots',
        'blade_mail',
        'arcane_boots',
        'black_king_bar',
        'desolator',
        'magic_wand',
        'tranquil_boots',
        'invis_sword',
        'echo_sabre',
        'manta',
        'ring_of_aquila',
        'force_staff',
        'aether_lens',
        'wind_lace',
        'silver_edge',
        'hand_of_midas',
        'sphere',
        'boots',
        'hurricane_pike',
        'bottle'
    ]
    column_names = ['id']
    for item in core_items:
        column_names.append("t_item_" + item)
    items = pd.DataFrame(columns=column_names)
    for i, row in df_matches.iterrows():
        purchase_list = row['purchase_log']
        for item in core_items:
            column_name = "t_item_" + item
            t_item[column_name] = np.nan

        if purchase_list is not None:
            for purchase in purchase_list:
                for item in core_items:
                    column_name = "t_item_" + item
                    if purchase['key'] == item:
                        t_item[column_name] = purchase['time']
                        unique_id = '{}_{}'.format(row['match_id'], row['account_id'])
                        t_item['id'] = unique_id
            items_row = pd.DataFrame.from_dict(t_item, orient='index').transpose()
            items = items.append(items_row, ignore_index=True)
    return items


def create_first_kill_timing(df_matches):
    kill_logs = df_matches['kills_log']
    t_first_kills = []
    for kill_log in kill_logs:
        if kill_log:
            t_first_kill = kill_log[0]['time']
            t_first_kills.append(t_first_kill)
        else:
            t_first_kills.append(None)
    return t_first_kills


# log = [{'time': 1}, {'time': 2}, {'time': 6}] -->
# --> aggregations = {'t_<log_name>_5_cnt': 2, 't_<log_name>_10_cnt': 1, ...}
def aggregate_by_times(log, log_name, times=None):
    aggregations = {}
    if times is None:
        max_min = 31
        period = 5
        times = [t for t in range(max_min) if t % period == 0]
    for agg_type in ('cnt', 'rpm'):
        aggregations.update({
            't_{}_{}_{}'.format(log_name, t, agg_type): 0
            for t in times
        })
        for record in log:
            for t in times:
                if record['time'] <= t * 60:
                    agg_name = 't_{}_{}_{}'.format(log_name, t, agg_type)
                    if agg_type == 'cnt':
                        aggregations[agg_name] += 1
                    elif agg_type == 'rpm':
                        aggregations[agg_name] += 1 / float(t if t != 0 else 1)
    return aggregations


def create_aggregations_from_logs(df_matches):
    df_observers = df_matches['obs_log'].apply(lambda log: aggregate_by_times(log, 'obs')).apply(pd.Series)
    df_sentries = df_matches['sen_log'].apply(lambda log: aggregate_by_times(log, 'sen')).apply(pd.Series)
    df_runes = df_matches['runes_log'].apply(lambda log: aggregate_by_times(log, 'runes')).apply(pd.Series)
    df_buyback = df_matches['buyback_log'].apply(lambda log: aggregate_by_times(log, 'buybacks'))
    df_kills = df_matches['kills_log'].apply(lambda log: aggregate_by_times(log, 'kills')).apply(pd.Series)
    df_gold_reasons = df_matches['gold_reasons'].apply(pd.Series)
    for col in df_gold_reasons.columns:
        df_gold_reasons.rename(columns={col: '{}_{}'.format('gold_reason', col)}, inplace=True)
    df_xp_reasons = df_matches['xp_reasons'].apply(pd.Series)
    for col in df_xp_reasons.columns:
        df_xp_reasons.rename(columns={col: '{}_{}'.format('xp_reason', col)}, inplace=True)
    df_kill_streaks = df_matches['kill_streaks'].apply(pd.Series)
    for col in df_kill_streaks.columns:
        df_kill_streaks.rename(columns={col: '{}_{}'.format('kill_streaks', col)}, inplace=True)
    #TODO: add the rest jsons
    df_matches = pd.concat([
        df_matches, df_observers, df_sentries,
        df_runes, df_buyback, df_kills,
        df_gold_reasons, df_xp_reasons
    ], axis=1)
    return df_matches


def create_dataset():
    df_matches = query_opendota(QUERY)
    df_matches['id'] = create_unique_id(df_matches)
    df_matches['datetime'] = df_matches["start_time"].apply(
        lambda x:
        datetime.datetime.fromtimestamp(x).strftime('%d.%m.%Y %H:%M:%S'))
    items = create_core_items_timings(df_matches)
    df_matches = df_matches.merge(items, left_on='id', right_on='id', how='inner')
    df_matches['t_first_kill'] = create_first_kill_timing(df_matches)
    df_matches = create_aggregations_from_logs(df_matches)


    print(df_matches.head())
    for col in list(df_matches):
        print(col)


if __name__ == '__main__':
    create_dataset()
