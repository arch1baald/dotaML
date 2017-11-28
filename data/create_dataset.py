import sys
import io
import datetime

import requests
import numpy as np
import pandas as pd


QUERY = None
with open('query.sql', 'r') as sql_file:
    QUERY = sql_file.read()

# For missings
INTEGER_MISSING = -666
DEFAULT_VALUES = {
    't_': INTEGER_MISSING,
    'is_roaming': False,
    'team_id': INTEGER_MISSING,
    'pings': INTEGER_MISSING,
    'gold_': INTEGER_MISSING,
    'xp_': INTEGER_MISSING,
    'kill_': INTEGER_MISSING,
    'multi_': INTEGER_MISSING,
    'hero_': 0,
}


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
        sys.stderr.write(data.get('error'))
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


# Calculates the number of events in log in fixed time range
# log = [{'time': 1}, {'time': 2}, {'time': 6}] -->
# --> aggregations = {'t_<log_name>_5_cnt': 2, 't_<log_name>_10_cnt': 1, ...}
def aggregate_by_times(log, log_name, times=None):
    aggregations = {}
    if times is None:
        max_min = 31
        period = 3
        times = list(range(0, max_min, period))
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


# From column of dicts the function creates columns with dict keys as names
def create_aggregations_from_logs(df_matches):
    df_observers = df_matches['obs_log'].apply(lambda log: aggregate_by_times(log, 'obs')).apply(pd.Series)
    df_sentries = df_matches['sen_log'].apply(lambda log: aggregate_by_times(log, 'sen')).apply(pd.Series)
    df_runes = df_matches['runes_log'].apply(lambda log: aggregate_by_times(log, 'runes')).apply(pd.Series)
    df_buyback = df_matches['buyback_log'].apply(lambda log: aggregate_by_times(log, 'buybacks'))
    df_kills = df_matches['kills_log'].apply(lambda log: aggregate_by_times(log, 'kills')).apply(pd.Series)
    df_gold_reasons = df_matches['gold_reasons'].apply(pd.Series)
    df_hero_roles = df_matches['role_log'].apply(
        lambda roles:
        {'hero_role_{}'.format(role.lower()): 1 for role in roles}
    ).apply(pd.Series)
    for col in df_gold_reasons.columns:
        df_gold_reasons.rename(columns={col: '{}_{}'.format('gold_reason', col)}, inplace=True)
    df_xp_reasons = df_matches['xp_reasons'].apply(pd.Series)
    for col in df_xp_reasons.columns:
        df_xp_reasons.rename(columns={col: '{}_{}'.format('xp_reason', col)}, inplace=True)
    df_kill_streaks = df_matches['kill_streaks'].apply(pd.Series)
    for col in df_kill_streaks.columns:
        # We should skip kill streaks longer then 11 because of rare values
        if int(col) > 11:
            df_kill_streaks.drop(col, axis=1, inplace=True)
        else:
            df_kill_streaks.rename(columns={col: '{}_{}'.format('kill_streak', col)}, inplace=True)
    df_multi_kills = df_matches['multi_kills'].apply(pd.Series)
    for col in df_multi_kills.columns:
        df_multi_kills.rename(columns={col: '{}_{}'.format('multi_kill', col)}, inplace=True)

    df_matches['pings'] = df_matches['ping_log'].apply(lambda dct: dct.get('0'))
    #TODO: add the rest jsons
    df_matches = pd.concat([
        df_matches, df_observers, df_sentries,
        df_runes, df_buyback, df_kills,
        df_gold_reasons, df_xp_reasons, df_kill_streaks,
        df_multi_kills, df_hero_roles,
    ], axis=1)
    df_matches.drop([
        'obs_log', 'sen_log',
        'runes_log', 'buyback_log', 'kills_log',
        'gold_reasons', 'xp_reasons', 'kill_streaks',
        'multi_kills', 'ping_log', 'role_log'
    ], axis=1, inplace=True)
    return df_matches


def create_hero_stats_table():
    hero_stats_url = 'https://api.opendota.com/api/heroStats'
    resp = requests.get(hero_stats_url)
    data = resp.json()
    if resp.status_code == 400 and data is not None:
        sys.stderr.write(data.get('error'))
    resp.raise_for_status()
    df_heroes = pd.DataFrame(data)
    df_heroes.drop([
        'id', 'icon', 'img', 'name', 'roles',
        'attack_type', 'localized_name', 'primary_attr'
    ], axis=1, inplace=True)
    df_heroes = df_heroes.fillna(0)
    return df_heroes


def fill_missings(df_matches):
    columns = df_matches.columns
    for col in columns:
        for key, column_default_value in DEFAULT_VALUES.items():
            if col.startswith(key):
                df_matches[col].fillna(column_default_value, inplace=True)
    return df_matches


def create_dataset():
    if QUERY is None:
        return
    df_matches = query_opendota(QUERY)
    df_matches['id'] = create_unique_id(df_matches)
    df_matches['datetime'] = df_matches["start_time"].apply(
        lambda x:
        datetime.datetime.fromtimestamp(x).strftime('%d.%m.%Y %H:%M:%S'))
    items = create_core_items_timings(df_matches)
    df_matches = df_matches.merge(items, left_on='id', right_on='id', how='inner', copy=False)
    df_matches.drop('purchase_log', axis=1, inplace=True)
    df_matches['t_first_kill'] = create_first_kill_timing(df_matches)
    df_matches = create_aggregations_from_logs(df_matches)
    df_heroes = create_hero_stats_table()
    df_matches = df_matches.merge(df_heroes, on='hero_id', how='left', suffixes=('', ''), copy=False)
    df_matches = fill_missings(df_matches)

    df_matches.to_csv('dataset.csv')


if __name__ == '__main__':
    create_dataset()
