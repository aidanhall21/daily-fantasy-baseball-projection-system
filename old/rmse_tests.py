#%%

import pandas as pd
import os
import datetime
import numpy as np
from numpy.polynomial.polynomial import polyfit
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)

def calculate_rmse(batters_df):
    
    dates_list = list(batters_df.Day.unique())
    
    for date in dates_list:
        
        #print(date)
        day_df = batters_df.loc[batters_df['Day'] == date]
        total_pa = day_df.PlateAppearances.sum()
        day_df['w'] = day_df.apply(lambda row: row['PlateAppearances'] / total_pa, axis=1)
        #day_df['obs_dkpa'] = day_df.apply(lambda row: row['draft_kings_points'] / row['PA'], axis=1)
        day_df['err'] = day_df.apply(lambda row: row['DKp/PA'] - row['DraftKingsPointsPerPA'], axis=1)
        day_df['sq_err'] = day_df.apply(lambda row: row['err'] ** 2, axis=1)
        day_df['weighted_sq_err'] = day_df.apply(lambda row: row['sq_err'] * row['w'], axis=1)
        
        rmse = sum(day_df['weighted_sq_err'])
        
        rmse_summary[date] = round(rmse, 3)
        
    return rmse_summary

# Start Here

def get_x_y(year):

    cwd = os.getcwd()
    os.chdir(cwd+'/Fantasy.2018-2021')

    game_stats = pd.read_csv('PlayerGame.' + str(year) + '.csv')

    reg_season_game_stats = game_stats.loc[game_stats.SeasonType == 1].reset_index(drop=True)
    reg_seas_batter_stats = reg_season_game_stats.loc[reg_season_game_stats.PositionCategory != 'P'].reset_index(drop=True)
    reg_seas_batter_stats['PlateAppearances'] = reg_seas_batter_stats.apply(lambda row: row['AtBats'] + row['Walks'] + row['HitByPitch'] + row['Sacrifices'], axis=1)
    reg_seas_batter_stats_nozeros = reg_seas_batter_stats.loc[reg_seas_batter_stats.PlateAppearances > 0]
    reg_seas_batter_stats_nozeros['DraftKingsPointsPerPA'] = reg_seas_batter_stats_nozeros.apply(lambda row: row['FantasyPointsDraftKings'] / row['PlateAppearances'], axis=1)

    os.chdir('..')
    os.chdir(cwd+'/BaselineProjections')
    projections = pd.read_csv('marcel_batters_' + str(year) + '.csv')

    projections['Name'] = projections.apply(lambda row: row['First'] + ' ' + row['Last'], axis=1)
    dkpa = projections[['Name', 'DKp/PA']]

    data = reg_seas_batter_stats_nozeros.merge(dkpa, how='left', on='Name')
    data = data[data['DKp/PA'].notna()]

    data = data.sort_values(['Day'], ascending=True).reset_index(drop=True)
    data['Day'] = data['Day'].astype('datetime64[ns]')

    rmse_dict = {}  
    rmse_dict = calculate_rmse(data)

    y_values = []
    k = list(rmse_dict.keys())
    k.sort()
    for i in k: y_values.append(rmse_dict[i])
    y = np.array(y_values)

    x_values = list(rmse_dict.keys())
    x = np.arange(len(x_values))
    b, m = polyfit(x, y, 1)

    return x, y, b, m
# %%
