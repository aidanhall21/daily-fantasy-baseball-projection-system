### RUN python dfs_get_data.py FIRST


import pandas as pd
import numpy as np
import math
from datetime import date
import json
from pybaseball import playerid_lookup
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
import unidecode


from dfs_functions import *

today = date.today()
d = today.strftime("%Y-%b-%d")

sals, players = get_batter_salaries(d)
sals_p, players_p = get_pitcher_salaries(d)

game_logs = get_current_season_game_logs_batters(d)
game_logs_p = get_current_season_game_logs_pitchers(d)

league_stats = get_league_stats(d)

league_hbp = league_stats.HitByPitch.sum()
league_hr = league_stats.HomeRuns.sum()
league_bb = league_stats.Walks.sum()
league_so = league_stats.Strikeouts.sum()
league_innings = game_logs_p.InningsPitchedDecimal.sum()
league_ER = game_logs_p.ER.sum()

marcels = get_marcels_batters(d)
marcels_p = get_marcels_pitchers(d)

df = marcels[['Name', 'Reliability']]
game_logs = game_logs.merge(df, how='left', on='Name')
game_logs = game_logs.loc[game_logs['PlayerID'].isin(players)]
sum_data = game_logs[['PlayerID', 'PA', 'S', 'D', 'T', 'HR', 'BB', 'HP', 'SB', 'CS', 'SO']].reset_index(drop=True).groupby(['PlayerID']).sum()

reliability_dict = {}

df_p = marcels_p[['Name', 'Reliability']]
game_logs_p = game_logs_p.merge(df_p, how='left', on='Name')
game_logs_p = game_logs_p.loc[game_logs_p['PlayerID'].isin(players_p)]
sum_data_p = game_logs_p[['PlayerID', 'Started', 'Games', 'W', 'TotalOutsPitched', 'ER', 'BB', 'SO', 'H', 'HR', 'H-HR']].reset_index(drop=True).groupby(['PlayerID']).sum()

reliability_dict_p = {}

for index, row in game_logs_p.iterrows():
    if math.isnan(row['Reliability']) == True:
        reliability_dict_p[row['PlayerID']] = 0
    else:
        reliability_dict_p[row['PlayerID']] = row['Reliability']

for index, row in game_logs.iterrows():
    if math.isnan(row['Reliability']) == True:
        reliability_dict[row['PlayerID']] = 0
    else:
        reliability_dict[row['PlayerID']] = row['Reliability']

player_stabilization_dict = create_stabilization_dict_hitters(sum_data, reliability_dict)
pitcher_stabilization_dict = create_stabilization_dict_pitchers(sum_data_p, reliability_dict_p)

marcels_dict, marcel_players = create_per_pa_marcels_rates_hitters(game_logs, marcels)
marcels_dict_p, marcel_pitchers = create_per_pa_marcels_rates_pitchers(game_logs_p, marcels_p)

new = sals['OperatorPosition'].str.split('/', n = 1, expand = True)
sals['EffectivePosition'] = new[0]

new_p = sals_p['OperatorPosition'].str.split('/', n = 1, expand = True)
sals_p['EffectivePosition'] = new_p[0]

average_stats_by_position = get_average_stats_by_position(d, game_logs, 'hitters')
average_stats_by_position_p = get_average_stats_by_position(d, game_logs_p, 'pitchers')

blended_projections_dict = create_blended_projections_hitters(players, marcel_players, player_stabilization_dict, marcels_dict, sals, average_stats_by_position)
blended_projections_dict_p = create_blended_projections_pitchers(players_p, marcel_pitchers, pitcher_stabilization_dict, marcels_dict_p, sum_data_p, average_stats_by_position_p)

sals['pS/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['S'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
sals['pD/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['D'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
sals['pT/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['T'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
sals['pHR/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['HR'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
sals['pBB/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['BB'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
sals['pHP/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['HP'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
sals['pSB/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['SB'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
sals['pSO/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['SO'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)

sals_p['pSO/Out'] = sals_p.apply(lambda row: round(blended_projections_dict_p[row['PlayerID']]['SO'], 3) if row['PlayerID'] in blended_projections_dict_p else np.NaN, axis=1)
sals_p['pBB/Out'] = sals_p.apply(lambda row: round(blended_projections_dict_p[row['PlayerID']]['BB'], 3) if row['PlayerID'] in blended_projections_dict_p else np.NaN, axis=1)
sals_p['pHR/Out'] = sals_p.apply(lambda row: round(blended_projections_dict_p[row['PlayerID']]['HR'], 3) if row['PlayerID'] in blended_projections_dict_p else np.NaN, axis=1)
sals_p['pH-HR/Out'] = sals_p.apply(lambda row: round(blended_projections_dict_p[row['PlayerID']]['H-HR'], 3) if row['PlayerID'] in blended_projections_dict_p else np.NaN, axis=1)
sals_p['pHBP/Out'] = sals_p.apply(lambda row: round(league_hbp / (league_innings * 3), 3), axis=1)

league_ERA = (9 / league_innings) * league_ER
FIP_constant = league_ERA - (((13 * league_hr) + (3 * (league_bb + league_hbp)) - (2 * league_so)) / league_innings)

game_stats_prior = get_prior_season_game_logs(d)

prior_year_ind_pitcher_dist = game_stats_prior.groupby('PlayerID').TotalOutsPitched.agg(['sum', 'mean', 'std']).fillna(0)
prior_year_league_innings_dist = game_stats_prior.TotalOutsPitched.agg(['sum', 'mean', 'std'])
current_year_starts = game_logs.loc[game_logs.Started == 1].reset_index(drop=True)
current_year_ind_pitcher_dist = current_year_starts.groupby('PlayerID').TotalOutsPitched.agg(['sum', 'mean', 'std']).fillna(0)
current_year_league_innings_dist = current_year_starts.TotalOutsPitched.agg(['sum', 'mean', 'std'])
current_year_outs = current_year_league_innings_dist['sum']
weighted_league_innings_dist_mean = ((current_year_league_innings_dist['mean'] * current_year_outs) + (prior_year_league_innings_dist['mean'] * 10000)) / (current_year_outs + 10000)
weighted_league_innings_dist_std = ((current_year_league_innings_dist['std'] * current_year_outs) + (prior_year_league_innings_dist['std'] * 10000)) / (current_year_outs + 10000)
current_year_starts_vs_team = game_logs.loc[game_logs.Started == 1].groupby('OpponentID').TotalOutsPitched.agg(['sum', 'mean', 'std']).fillna(0)

sals_with_vegas_lines, starting_pitchers = get_vegas_lines(d, sals)
sals_with_vegas_lines_p, starting_pitchers = get_vegas_lines(d, sals_p)

pa_sals_with_vegas_lines = adjust_for_park_factors(sals_with_vegas_lines)
pitcher_sals_with_vegas_lines = adjust_for_park_factors_pitchers(sals_with_vegas_lines_p)

## NEED TO FIGURE THE BATTING ORDER PART OUT
#batting_order_file = get_batting_orders_file()

pa_sals_with_vegas_lines['battingorderposition'] = 2

sals_with_batting_order = apply_starters_obp(pa_sals_with_vegas_lines)

sals_with_batting_order['pPA'] = sals_with_batting_order.apply(lambda row: 1 if pd.isnull(row['battingorderposition']) else round(3.3 + (-0.12 * row['battingorderposition']) + (.036 * row['PlayerTeamTotal']) + (3.92 * row['startersOBP']), 2), axis=1)
sals_with_batting_order['OBP-HR'] = sals_with_batting_order.apply(lambda row: row['pS/PA'] + row['pD/PA'] + row['pT/PA'] + row['pBB/PA'] + row['pHP/PA'], axis=1)
sals_with_batting_order['pAB/PA'] = sals_with_batting_order.apply(lambda row: 1 - row['pBB/PA'] - row['pHP/PA'], axis=1)

#lead_hitters_obp_dict = find_lead_hitters_obp(sals_with_batting_order)
#trail_hitters_ops_dict = find_trail_hitters_ops(sals_with_batting_order)

all_starters_projections_dict = generate_starting_pitcher_projections(starting_pitchers, pitcher_sals_with_vegas_lines, prior_year_ind_pitcher_dist, prior_year_league_innings_dist, current_year_ind_pitcher_dist, current_year_league_innings_dist, current_year_outs, weighted_league_innings_dist_mean, weighted_league_innings_dist_std, current_year_starts_vs_team, FIP_constant)

projection_df = generate_projection_df_hitters(sals_with_batting_order)
projection_df_p = generate_projection_df_pitchers(pitcher_sals_with_vegas_lines, all_starters_projections_dict, FIP_constant)

p = projection_df_p[['PlayerID', 'TeamID', 'OperatorPlayerName', 'pIP', 'pK', 'pBB', 'pHR', 'pH', 'pHBP', 'pBF']]
p = p[p.pIP > 1]
p.drop_duplicates(subset=None, keep='first', inplace=True)
p = p.reset_index(drop=True)

b = projection_df[['PlayerID', 'TeamID', 'OperatorPlayerName', 'pPA', 'pR', 'pS', 'pD', 'pT', 'pHR', 'pRBI', 'pBB', 'pHP', 'pSO', 'Opponent_ID']]
b.drop_duplicates(subset=None, keep='first', inplace=True)
b = b.reset_index(drop=True)

t = b.merge(p, left_on='Opponent_ID', right_on='TeamID')

lg_a = game_logs[['PA', 'S', 'D', 'T', 'HR', 'BB', 'HP', 'SO']].reset_index(drop=True).mean().tolist()
a_pa = lg_a[0]
lg_p = [item / a_pa for item in lg_a]
lg_a_h = lg_p[1] + lg_p[2] + lg_p[3] + lg_p[4]
lg_p.append(lg_a_h)

or_stats = ['HR', 'BB', 'HBP', 'SO', 'H']
lg_a_or_dict = {}
for i in range(len(or_stats)):
    lg_a_or_dict[or_stats[i]] = lg_p[i + 4] / (1 - lg_p[i + 4])

or_stats = ['HR', 'BB', 'HBP', 'SO', 'H']
lg_a_or_dict = {}
for i in range(len(or_stats)):
    lg_a_or_dict[or_stats[i]] = lg_p[i + 4] / (1 - lg_p[i + 4])

team_totals = {}
OR_pitchers = {}
pitcher_index = {}

for r in t.index:
    row = t.loc[r,:].tolist()
    op = row[16]
    pid = row[14]
    team = row[15]
    ip = row[17]
    if op in pitcher_index: pass
    else:
        pitcher_index[op] = [pid, team, ip]

# OR for pitchers
for r in t.index:
    row = t.loc[r, :].tolist()
    n = row[16]
    
    bf = row[23]
    
    k_p = row[18] / bf
    hr_p = row[20] / bf
    bb_p = row[19] / bf
    h_p = row[21] / bf
    hbp_p = row[22] / bf
    
    p_or_k = k_p / (1 - k_p)
    p_or_hr = hr_p / (1 - hr_p)
    p_or_h = h_p / (1 - h_p)
    p_or_bb = bb_p / (1 - bb_p)
    p_or_hbp = hbp_p / (1 - hbp_p)    
    
    if n in OR_pitchers: pass
    else:
        OR_pitchers[n] = [p_or_k, p_or_hr, p_or_h, p_or_bb, p_or_hbp, bf]
        
for r in t.index:
    row = t.loc[r, :].tolist()
    batter_team = row[1]
    if np.isnan(batter_team): continue
    else:
        # get total team projected outs
        pa = row[3]
        hits = row[5] + row[6] + row[7] + row[8]
        outs = pa - hits - row[10] - row[11]

        if np.isnan(outs) or np.isnan(pa): continue
        elif batter_team in team_totals:
            team_totals[batter_team][0] += pa
            team_totals[batter_team][1] += outs
        else: team_totals[batter_team] = [pa, outs]

stats = ['SO', 'HR', 'H', 'BB', 'HBP']
r_lst = []

adj_p_dict = {}

for r in t.index:
    row = t.loc[r,:].tolist()
    pa = row[3]
    tm = row[1]
    if np.isnan(tm) or np.isnan(row[4]): pass
    else:
        hits = row[5] + row[6] + row[7] + row[8]

        # find odds ratio for K, HR, H, HBP, BB

        k_p = row[12] / pa
        hr_p = row[8] / pa
        bb_p = row[10] / pa
        h_p = hits / pa
        hbp_p = row[11] / pa

        b_or_k = k_p / (1 - k_p)
        b_or_hr = hr_p / (1 - hr_p)
        b_or_h = h_p / (1 - h_p)
        b_or_bb = bb_p / (1 - bb_p)
        b_or_hbp = hbp_p / (1 - hbp_p)

        b_or = [b_or_k, b_or_hr, b_or_h, b_or_bb, b_or_hbp]

        for i in range(5):
            op = row[16]
            if op in adj_p_dict: pass
            else:
                adj_p_dict[op] = [0, 0, 0, 0, 0]

            adj_b_or = (b_or[i] * OR_pitchers[op][i]) / lg_a_or_dict[stats[i]]
            adj_b_p = adj_b_or / (adj_b_or + 1)
            ns = adj_b_p * pa

            ps = round(ns / (team_totals[tm][0] / OR_pitchers[op][5]), 2)

            pavspp = OR_pitchers[op][5] / (team_totals[tm][0] / (team_totals[tm][1] / 27))
            ns_sp = ns * pavspp
            ns_rp = (b_or[i] / (b_or[i] + 1)) * pa * (1 - pavspp)

            adj_p_dict[op][i] += ps

            ns_t = round(ns_rp + ns_sp, 2)

            row.append(ns_t)

        r_lst.append(row)   
        
g = pd.DataFrame(r_lst)
g.columns = t.columns.tolist() + ['nK', 'nHR', 'nH', 'nBB', 'nHBP']
g = g[['PlayerID_x', 'TeamID_x', 'OperatorPlayerName_x', 'nK', 'nHR', 'nH', 'nBB', 'nHBP']]
g = g.rename(columns={"PlayerID_x": "PlayerID", "TeamID_x": "TeamID", 'OperatorPlayerName_x': 'Name', 'nK': 'K', 'nHR': 'HR', 'nH': 'H', 'nBB': 'BB', 'nHBP': 'HBP'})

b_info = projection_df[['PlayerID', 'SlateID', 'Operator', 'OperatorPlayerID', 'OperatorSalary', 'OperatorGameType', 'OperatorRosterSlots', 'pPA', 'pR', 'pS', 'pD', 'pT', 'pRBI', 'pSB']]
b_info = b_info.rename(columns={"pPA": "PA", "pR": "R", 'pRBI': 'RBI', 'pSB': 'SB'})
b_info = b_info[b_info['OperatorGameType'] == 'Classic'].reset_index(drop=True)

batters_final_df = g.merge(b_info, how='left', on='PlayerID')

batters_final_df['1B'] = batters_final_df.apply(lambda row: (row['pS'] / (row['pS'] + row['pD'] + row['pT'])) * (row['H'] - row['HR']) , axis=1)
batters_final_df['2B'] = batters_final_df.apply(lambda row: (row['pD'] / (row['pS'] + row['pD'] + row['pT'])) * (row['H'] - row['HR']) , axis=1)
batters_final_df['3B'] = batters_final_df.apply(lambda row: (row['pT'] / (row['pS'] + row['pD'] + row['pT'])) * (row['H'] - row['HR']) , axis=1)
batters_final_df['DraftKingsPoints'] = batters_final_df.apply(lambda row: round((3 * row['1B']) + (5 * row['2B']) + (8 * row['3B']) + (10 * row['HR']) + (2 * row['RBI']) + (2 * row['R']) + (2 * row['BB']) + (2 * row['HBP']) + (5 * row['SB']), 2), axis=1)
batters_final_df['FanDuelPoints'] = batters_final_df.apply(lambda row: round((3 * row['1B']) + (6 * row['2B']) + (9 * row['3B']) + (12 * row['HR']) + (3.5 * row['RBI']) + (3.2 * row['R']) + (3 * row['BB']) + (3 * row['HBP']) + (6 * row['SB']), 2), axis=1)

bfd = batters_final_df[['PlayerID', 'TeamID', 'SlateID', 'Operator', 'OperatorPlayerID', 'OperatorSalary', 'OperatorGameType', 'Name', 'OperatorRosterSlots', 'PA', 'H', 'R', '1B', '2B', '3B', 'HR', 'RBI', 'K', 'BB', 'HBP', 'SB', 'DraftKingsPoints', 'FanDuelPoints']]

for key in pitcher_index:
    adj_p_dict[key] += pitcher_index[key]

v = pd.DataFrame.from_dict(adj_p_dict, orient='index')
v = v.reset_index()
v.columns = ['Name', 'K', 'HR', 'H', 'BB', 'HBP', 'PlayerID', 'TeamID', 'IP']

p_info = projection_df_p[['PlayerID', 'SlateID', 'Operator', 'OperatorPlayerID', 'OperatorSalary', 'OperatorGameType', 'OperatorRosterSlots', 'pW', 'pQS']]
p_info = p_info[p_info['OperatorGameType'] == 'Classic'].reset_index(drop=True)
pitchers_final_df = v.merge(p_info, how='left', on='PlayerID')

pitchers_final_df['ER'] = pitchers_final_df.apply(lambda row: round(((((13 * row['HR']) + (3 * (row['BB'] + row['HBP'])) - (2 * row['K'])) / row['IP']) + FIP_constant) * (row['IP'] / 9), 2), axis=1)
pitchers_final_df['TBF'] = pitchers_final_df.apply(lambda row: round(3 * row['IP'] + row['BB'] + row['H'] + row['HBP'], 2), axis=1)
pitchers_final_df['DraftKingsPoints'] = pitchers_final_df.apply(lambda row: round(row['IP'] * 2.25 + row['K'] * 2 + row['pW'] * 4 + row['ER'] * -2 + row['H'] * -0.6 + row['BB'] * -0.6 + row['HBP'] * -0.6, 2), axis=1)
pitchers_final_df['FanDuelPoints'] = pitchers_final_df.apply(lambda row: round(row['pW'] * 6 + row['pQS'] * 4 + row['ER'] * -3 + row['K'] * 3 + row['IP'] * 3, 2), axis=1)

pfd = pitchers_final_df.rename(columns={"pW": "W", "pQS": "QS"})
pfd = pfd[['PlayerID', 'TeamID', 'SlateID', 'Operator', 'OperatorPlayerID', 'OperatorSalary', 'OperatorGameType', 'Name', 'OperatorRosterSlots', 'IP', 'TBF', 'W', 'QS', 'H', 'ER', 'HR', 'K', 'BB', 'HBP', 'DraftKingsPoints', 'FanDuelPoints']]

pfd['DK$'] = pfd.apply(lambda r: round(r['DraftKingsPoints'] / (r['OperatorSalary'] / 1000), 2) , axis=1)
pfd['FD$'] = pfd.apply(lambda r: round(r['FanDuelPoints'] / (r['OperatorSalary'] / 1000), 2) , axis=1)

bfd['TB'] = bfd.apply(lambda r: round(r['1B'] + 2 * r['2B'] + 3 * r['3B'] + 4 * r['HR'], 2), axis=1)
bfd['DK$'] = bfd.apply(lambda r: round(r['DraftKingsPoints'] / (r['OperatorSalary'] / 1000), 2) , axis=1)
bfd['FD$'] = bfd.apply(lambda r: round(r['FanDuelPoints'] / (r['OperatorSalary'] / 1000), 2) , axis=1)

pid_map = pd.read_csv('player_id_map.csv')
pid_map = pid_map[['mlbname', 'mlbid']].dropna()
pid_map2 = pd.read_csv('names_id_df.csv')
comb_ids = pd.concat([pid_map, pid_map2]).reset_index(drop=True)

bfd['Name_u'] = bfd.apply(lambda r: unidecode.unidecode(r['Name']), axis=1)
pfd['Name_u'] = pfd.apply(lambda r: unidecode.unidecode(r['Name']), axis=1)

pfd = pfd.merge(comb_ids, how='left', left_on='Name_u', right_on='mlbname')
bfd = bfd.merge(comb_ids, how='left', left_on='Name_u', right_on='mlbname')


names_to_lookup = []

for i in range(len(pfd.index)):
    r = pfd.loc[i, ['Name', 'mlbid']].tolist()
    name = unidecode.unidecode(r[0])
    if np.isnan(r[1]) and name not in names_to_lookup: names_to_lookup.append(name) 
    else: continue
        
for i in range(len(bfd.index)):
    r = bfd.loc[i, ['Name', 'mlbid']].tolist()
    name = unidecode.unidecode(r[0])
    if np.isnan(r[1]) and name not in names_to_lookup: names_to_lookup.append(name) 
    else: continue
        
additional_names = []

for n in names_to_lookup:
    ns = n.split(' ')
    try:
        data = playerid_lookup(ns[1], ns[0])
        pid = data['key_mlbam'][0]
    except: continue
    additional_names.append([n, pid])
    
additional_names_df = pd.DataFrame(additional_names, columns = ['mlbname', 'mlbid'])
comb_df = pd.concat([pid_map2, additional_names_df]).reset_index(drop=True)
comb_df.to_csv('names_id_df.csv', index=False)

pfd = pfd.merge(additional_names_df, how='left', left_on='Name_u', right_on='mlbname')
bfd = bfd.merge(additional_names_df, how='left', left_on='Name_u', right_on='mlbname')

bfd['pid'] = bfd['mlbid_x'].combine_first(bfd['mlbid_y'])
pfd['pid'] = pfd['mlbid_x'].combine_first(pfd['mlbid_y'])

bfd = bfd.drop(['Name_u', 'mlbname_x', 'mlbid_x', 'mlbname_y', 'mlbid_y'], axis=1)
bfd = bfd.rename(columns={"PlayerID": "SportsDataIO_ID", "pid": "PlayerID"})

first_column = bfd.pop('PlayerID')
bfd.insert(0, 'PlayerID', first_column)

pfd = pfd.drop(['Name_u', 'mlbname_x', 'mlbid_x', 'mlbname_y', 'mlbid_y'], axis=1)
pfd = pfd.rename(columns={"PlayerID": "SportsDataIO_ID", "pid": "PlayerID"})

first_column = pfd.pop('PlayerID')
pfd.insert(0, 'PlayerID', first_column)

p_out = pfd.to_json(orient='index')
b_out = bfd.to_json(orient='index')

p_object = json.loads(p_out)
b_object = json.loads(b_out)

pfd.to_csv('pitchers_test.csv', index=False)
bfd.to_csv('batters_test.csv', index=False)



