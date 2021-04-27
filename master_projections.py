#%%
import pandas as pd
import os
import numpy as np
import math
from scipy.stats import pearsonr
import requests
from datetime import date
from datetime import datetime
from statistics import mean
from pulp import *
from collections import Counter

from projection_file_functions import *

date = '2019-AUG-21'

def get_hitter_projections_by_date(date):

    sals, players = get_batter_salaries(date)
    game_logs = get_current_season_game_logs_batters(date)
    marcels = get_marcels_batters(date)

    df = marcels[['Name', 'Reliability']]
    game_logs = game_logs.merge(df, how='left', on='Name')
    game_logs = game_logs.loc[game_logs['PlayerID'].isin(players)]
    sum_data = game_logs[['PlayerID', 'PA', 'S', 'D', 'T', 'HR', 'BB', 'HP', 'SB', 'CS']].reset_index(drop=True).groupby(['PlayerID']).sum()

    reliability_dict = {}

    for index, row in game_logs.iterrows():
        if math.isnan(row['Reliability']) == True:
            reliability_dict[row['PlayerID']] = 0
        else:
            reliability_dict[row['PlayerID']] = row['Reliability']

    player_stabilization_dict = create_stabilization_dict_hitters(sum_data, reliability_dict)

    marcels_dict, marcel_players = create_per_pa_marcels_rates_hitters(game_logs, marcels)

    new = sals['OperatorPosition'].str.split('/', n = 1, expand = True)
    sals['EffectivePosition'] = new[0]

    average_stats_by_position = get_average_stats_by_position(date, game_logs, 'hitters')

    blended_projections_dict = create_blended_projections_hitters(players, marcel_players, player_stabilization_dict, marcels_dict, sals, average_stats_by_position)

    sals['pS/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['S'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pD/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['D'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pT/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['T'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pHR/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['HR'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pBB/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['BB'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pHP/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['HP'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pSB/PA'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['SB'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)

    sals_with_vegas_lines, starting_pitchers = get_vegas_lines(date, sals)

    batting_order_file = get_batting_orders_file()

    sals_with_batting_order = sals_with_vegas_lines.merge(batting_order_file, how = 'left', on = ['PlayerID', 'GameID'])
    sals_with_batting_order = apply_starters_obp(sals_with_batting_order)
    sals_with_batting_order['pPA'] = sals_with_batting_order.apply(lambda row: 1 if pd.isnull(row['battingorderposition']) else round(3.3 + (-0.12 * row['battingorderposition']) + (.036 * row['PlayerTeamTotal']) + (3.92 * row['startersOBP']), 2), axis=1)
    sals_with_batting_order['OBP-HR'] = sals_with_batting_order.apply(lambda row: row['pS/PA'] + row['pD/PA'] + row['pT/PA'] + row['pBB/PA'] + row['pHP/PA'], axis=1)
    sals_with_batting_order['pAB/PA'] = sals_with_batting_order.apply(lambda row: 1 - row['pBB/PA'] - row['pHP/PA'], axis=1)

    lead_hitters_obp_dict = find_lead_hitters_obp(sals_with_batting_order)
    trail_hitters_ops_dict = find_trail_hitters_ops(sals_with_batting_order)
    sals_with_batting_order['leadhittersOBP-HR'] = sals_with_batting_order.apply(lambda row: lead_hitters_obp_dict[row['PlayerID']] if row['PlayerID'] in lead_hitters_obp_dict else 0, axis=1)
    sals_with_batting_order['trailhittersOPS'] = sals_with_batting_order.apply(lambda row: trail_hitters_ops_dict[row['PlayerID']] if row['PlayerID'] in trail_hitters_ops_dict else 0, axis=1)

    projection_df = generate_projection_df_hitters(sals_with_batting_order)

    print(f'generated hitter projections for {date}')
    return projection_df

def get_pitcher_projections_by_date(date):

    sals, players = get_pitcher_salaries(date)
    game_logs = get_current_season_game_logs_pitchers(date)
    league_stats = get_league_stats(date)

    league_hbp = league_stats.HitByPitch.sum()
    league_hr = league_stats.HomeRuns.sum()
    league_bb = league_stats.Walks.sum()
    league_so = league_stats.Strikeouts.sum()
    league_innings = game_logs.InningsPitchedDecimal.sum()
    league_ER = game_logs.ER.sum()

    marcels = get_marcels_pitchers(date)

    df = marcels[['Name', 'Reliability']]
    game_logs = game_logs.merge(df, how='left', on='Name')
    game_logs = game_logs.loc[game_logs['PlayerID'].isin(players)]
    sum_data = game_logs[['PlayerID', 'Started', 'Games', 'W', 'TotalOutsPitched', 'ER', 'BB', 'SO', 'H', 'HR', 'H-HR']].reset_index(drop=True).groupby(['PlayerID']).sum()

    reliability_dict = {}

    for index, row in game_logs.iterrows():
        if math.isnan(row['Reliability']) == True:
            reliability_dict[row['PlayerID']] = 0
        else:
            reliability_dict[row['PlayerID']] = row['Reliability']

    player_stabilization_dict = create_stabilization_dict_pitchers(sum_data, reliability_dict)

    marcels_dict, marcel_players = create_per_pa_marcels_rates_pitchers(game_logs, marcels)

    new = sals['OperatorPosition'].str.split('/', n = 1, expand = True)
    sals['EffectivePosition'] = new[0]

    average_stats_by_position = get_average_stats_by_position(date, game_logs, 'pitchers')

    blended_projections_dict = create_blended_projections_pitchers(players, marcel_players, player_stabilization_dict, marcels_dict, sum_data, average_stats_by_position)

    sals['pSO/Out'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['SO'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pBB/Out'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['BB'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pHR/Out'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['HR'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pH-HR/Out'] = sals.apply(lambda row: round(blended_projections_dict[row['PlayerID']]['H-HR'], 3) if row['PlayerID'] in blended_projections_dict else np.NaN, axis=1)
    sals['pHBP/Out'] = sals.apply(lambda row: round(league_hbp / (league_innings * 3), 3), axis=1)

    league_ERA = (9 / league_innings) * league_ER
    FIP_constant = league_ERA - (((13 * league_hr) + (3 * (league_bb + league_hbp)) - (2 * league_so)) / league_innings)

    game_stats_prior = get_prior_season_game_logs(date)

    prior_year_ind_pitcher_dist = game_stats_prior.groupby('PlayerID').TotalOutsPitched.agg(['sum', 'mean', 'std']).fillna(0)
    prior_year_league_innings_dist = game_stats_prior.TotalOutsPitched.agg(['sum', 'mean', 'std'])
    current_year_starts = game_logs.loc[game_logs.Started == 1].reset_index(drop=True)
    current_year_ind_pitcher_dist = current_year_starts.groupby('PlayerID').TotalOutsPitched.agg(['sum', 'mean', 'std']).fillna(0)
    current_year_league_innings_dist = current_year_starts.TotalOutsPitched.agg(['sum', 'mean', 'std'])
    current_year_outs = current_year_league_innings_dist['sum']
    weighted_league_innings_dist_mean = ((current_year_league_innings_dist['mean'] * current_year_outs) + (prior_year_league_innings_dist['mean'] * 10000)) / (current_year_outs + 10000)
    weighted_league_innings_dist_std = ((current_year_league_innings_dist['std'] * current_year_outs) + (prior_year_league_innings_dist['std'] * 10000)) / (current_year_outs + 10000)
    current_year_starts_vs_team = game_logs.loc[game_logs.Started == 1].groupby('OpponentID').TotalOutsPitched.agg(['sum', 'mean', 'std']).fillna(0)

    sals_with_vegas_lines, starting_pitchers = get_vegas_lines(date, sals)

    all_starters_projections_dict = generate_starting_pitcher_projections(starting_pitchers, sals_with_vegas_lines, prior_year_ind_pitcher_dist, prior_year_league_innings_dist, current_year_ind_pitcher_dist, current_year_league_innings_dist, current_year_outs, weighted_league_innings_dist_mean, weighted_league_innings_dist_std, current_year_starts_vs_team, FIP_constant)

    projection_df = generate_projection_df_pitchers(sals_with_vegas_lines, all_starters_projections_dict, FIP_constant)

    print(f'generated pitcher projections for {date}')
    return projection_df


### This stuff is all for accuracy testing

hitter_projections = get_hitter_projections_by_date(date)
pitcher_projections = get_pitcher_projections_by_date(date)

hitters_points_and_sal = hitter_projections.loc[:,['PlayerID','SlateID', 'OperatorPlayerName', 'OperatorPosition', 'OperatorSalary','DraftKingsPoints']]
pitchers_points_and_sal = pitcher_projections.loc[:,['PlayerID', 'SlateID', 'OperatorPlayerName', 'OperatorPosition', 'OperatorSalary','DraftKingsPoints']]
points_and_sal = pd.concat([hitters_points_and_sal, pitchers_points_and_sal], axis=0)

slates_list = get_classic_slates_by_date(date)

optimals_dict = {}

sports_data_projections = get_sportsdata_projections_by_date(date)

for slate in slates_list:

    print(slate)
    optimal_players, optimal_player_names, total_salary_used, projected_points = dfs_optimizer(slate, points_and_sal,'DraftKingsPoints', [], [])
    optimals_dict[slate] = {'players': optimal_players, 'names': optimal_player_names, 'salary': total_salary_used, 'proj_points': projected_points}
    
    actual_points = get_actual_points_scored_by_lineup(date, optimals_dict[slate]['players'])
    optimals_dict[slate]['actual_points'] = actual_points
    
    actual_optimal_players, actual_optimal_player_names, top_total_salary_used, max_points_scored = get_best_lineup(date, slate, 'FantasyPointsDraftKings', [], [])
    optimals_dict[slate]['optimal_players'] = actual_optimal_players
    optimals_dict[slate]['optimal_player_names'] = actual_optimal_player_names
    optimals_dict[slate]['real_total_sal'] = top_total_salary_used
    optimals_dict[slate]['max_points_score'] = max_points_scored

    common_player_names = list(set(optimal_player_names) & set(actual_optimal_player_names))
    common_player_count = [key for key, val in enumerate(optimal_players) if val in set(actual_optimal_players)]
    num_common_players = len(common_player_count)

    optimals_dict[slate]['common_player_names'] = common_player_names
    optimals_dict[slate]['num_common_players'] = num_common_players

    sd_optimal_players, sd_optimal_player_names, sd_total_salary_used, sd_projected_points = get_sportsdata_optimal(slate, sports_data_projections, 'FantasyPointsDraftKings', [], [])

    optimals_dict[slate]['sd_optimal_players'] = sd_optimal_players
    optimals_dict[slate]['sd_optimal_player_names'] = sd_optimal_player_names
    optimals_dict[slate]['sd_total_sal'] = sd_total_salary_used
    optimals_dict[slate]['sd_proj_points'] = sd_projected_points

    sd_optimal_actual_points = get_actual_points_scored_by_lineup(date, optimals_dict[slate]['sd_optimal_players'])
    optimals_dict[slate]['sd_actual_points'] = sd_optimal_actual_points
    win = 1 if actual_points > sd_optimal_actual_points else 0
    optimals_dict[slate]['projection_win'] = win
    diff = abs(actual_points - sd_optimal_actual_points)
    optimals_dict[slate]['score_dff'] = diff

    sd_common_player_names = list(set(sd_optimal_player_names) & set(optimal_player_names))
    sd_common_player_count = [key for key, val in enumerate(sd_optimal_players) if val in set(actual_optimal_players)]
    num_sd_common_players = len(sd_common_player_count)
    optimals_dict[slate]['sd_common_player_names'] = sd_common_player_names
    optimals_dict[slate]['sd_num_common_players'] = num_sd_common_players

    print('PL Optimal Score: ' + str(actual_points))
    print('SD Optimal Score: ' + str(sd_optimal_actual_points))
    if win == 1:
        print('WINNER')
    else:
        print('loser')
    


# %%

# %%
