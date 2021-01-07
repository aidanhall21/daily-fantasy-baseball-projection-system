#%%

#####
## This file will create baseline projections for each player for the upcoming season

import pandas as pd
import re
import requests
import sys
import psycopg2

from pybaseball import batting_stats

pd.set_option('display.max_columns', None)

def draft_king_batters(single, double, triple, HR, RBI, R, BB, HBP, SB):
    
    return (3 * single) + (5 * double) + (8 * triple) + (10 * HR) + (2 * RBI) + (2 * R) + (2 * BB) + (2 * HBP) + (5 * SB)

def best_ball_batters(AB, H, HR, R, RBI, SB):
    
    return (-1 * AB) + (4 * H) + (6 * HR) + (2 * R) + (2 * RBI) + (5 * SB)

def batting_average_batters(AB, singles, doubles, triples, HR):
    
    return round((singles + doubles + triples + HR) / AB, 3)

def onbase_percentage_batters(PA, singles, doubles, triples, HR, BB, HBP):
    
    return round((singles + doubles + triples + HR + BB + HBP) / PA, 3)

def slugging_percentage_batters(AB, singles, doubles, triples, HR):
    
    return round(((singles) + (2 * doubles) + (3 * triples) + (4 * HR)) / AB, 3)

## Download data from fangraphs
## year should be the upcoming season for which you would like to make projections

def preseason_projections_hitters(year):

    ## Get prior 3 seasons of data

    data = batting_stats(year - 3, year - 1, qual=1, split_seasons=True)

    data1 = data[data['Season'] == (year - 1)]
    data2 = data[data['Season'] == (year - 2)]
    data3 = data[data['Season'] == (year - 3)]

    ## If 2020 shortened season data is needed for projections, then import file projecting a full 2020 season

    if year == 2021:
        data1 = pd.read_csv('full_season_data_2020.csv')
    
    if year == 2022:
        data2 = pd.read_csv('full_season_data_2020.csv')

    if year = 2023:
        data3 = pd.read_csv('full_season_data_2020.csv')

    ## League statistics imported from file donwloaded directly from Fangraphs
    ## There could be a way to do this automatically and not have to rely on a direct download

    league_agg_stats = pd.read_csv('fangraphs_league_stats_1980-2020.csv')
    league_agg_stats.index = league_agg_stats['Season']

    ## List of relevant columns

    cols1 = ['PA', 'AB', 'R', '1B', '2B', '3B', 'HR', 'RBI', 'BB', 'SO', 'HBP', 'SF', 'SB']
    cols2 = ['IDfg', 'Name', 'Team', 'Age', 'Season']
    cols3 = ['IDfg', 'Name', 'Season'] + cols1
    player_info_prior_season = data1[cols2].reset_index(drop=True)

    ## Convert aggregate league stats to per plate appearance rates

    league_avgs_per_pa = league_agg_stats[cols1].div(league_agg_stats.PA, axis=0)

    league_avgs_per_pa = league_avgs_per_pa.loc[[year - 3, year - 2, year - 1]]

    ## Only projecting players who made an appearance in the prior season
    ## ***TODO: Will have to figure out in the future how to remove pitcher's batting stats and vice versa
    ## as well as dealing with players who opted out of the 2020 season

    players_from_prior_season = list(data1.IDfg)
    data2 = data2[data2['IDfg'].isin(players_from_prior_season)]
    data3 = data3[data3['IDfg'].isin(players_from_prior_season)]

    proj_df = pd.concat([data1[cols3].reset_index(drop=True), data2[cols3].reset_index(drop=True), data3[cols3].reset_index(drop=True)], axis=0)
    proj_df = proj_df.sort_values(by=['Name', 'Season'], ascending=True).reset_index(drop=True)

    ## Table with weights for past season data
    ## Weighting scheme is 5/4/3 for counting stats and 5/1/0 for playing time
    ## Methodology follows the Marcels projection system developed by Tom Tango and explained here
    ## http://tangotiger.net/marcel/
    ## and here
    ## http://www.tangotiger.net/archives/stud0346.shtml

    weight_df = pd.DataFrame({'PA': [3.0, 4, 5], 'AB': [3.0, 4, 5], 'R': [3.0, 4, 5], '1B': [3.0, 4, 5], '2B': [3.0, 4, 5], '3B': [3.0, 4, 5],
                     'HR': [3.0, 4, 5], 'RBI': [3.0, 4, 5], 'BB': [3.0, 4, 5], 'SO': [3.0, 4, 5], 'HBP': [3.0, 4, 5],
                    'SF': [3.0, 4, 5], 'SB': [3.0, 4, 5]}, index=[str(year - 3), str(year - 2), str(year - 1)])

    playing_time_weight_df = pd.DataFrame({'PA': [0.0, 0.1, 0.5]}, index=[str(year - 3), str(year - 2), str(year - 1)])

    all_years_list = list(weight_df.index)
    all_weighted_seasons = []
    all_league_weighted_seasons = []
    all_playing_time_weighted_seasons = []

    ## Could make this a function

    for pid in players_from_prior_season:
        
        ## This section applies weighting scheme to prior 3 seasons of data for each player
        
        df = proj_df[proj_df['IDfg'] == pid]
        
        player_pa = df['PA']
        
        partial_df = df.iloc[:, 2:]
        partial_df = partial_df.set_index(partial_df.columns[0])
        
        years_of_data = partial_df.shape[0]
        years_list = all_years_list[(3 - years_of_data):]
        
        adj_weight_df = weight_df.iloc[weight_df.index.isin(years_list)]
        
        weighted_df = pd.DataFrame(partial_df.values*adj_weight_df.values, columns=partial_df.columns, index=partial_df.index)
        
        partial_df = df.iloc[:, :3]
        weighted_season = pd.concat([partial_df.reset_index(drop=True), weighted_df.reset_index(drop=True)], axis=1)
        weighted_season['weights'] = 'player'
        weighted_season = weighted_season.drop(['Name', 'Season'], axis=1)
        
        all_weighted_seasons.append(weighted_season)
        
        ## This section finds expected rates for each player based on yearly league average rates
        
        player_pa_df = pd.concat([player_pa] * league_avgs_per_pa.shape[1], axis=1)
        
        adj_league_avgs_per_pa = league_avgs_per_pa.iloc[league_avgs_per_pa.index.isin(years_list)]

        expected_stats_by_player_pa = pd.DataFrame(adj_league_avgs_per_pa.values*player_pa_df.values, columns=adj_league_avgs_per_pa.columns, index=adj_league_avgs_per_pa.index)
        expected_stats_by_player_pa_weighted_df = pd.DataFrame(expected_stats_by_player_pa.values*adj_weight_df.values, columns=expected_stats_by_player_pa.columns, index=expected_stats_by_player_pa.index)
        
        expected_stats_by_player = pd.concat([partial_df.reset_index(drop=True), expected_stats_by_player_pa_weighted_df.reset_index(drop=True)], axis=1)
        
        expected_stats_by_player['weights'] = 'league'
        expected_stats_by_player = expected_stats_by_player.drop(['Name', 'Season'], axis=1)
        
        all_league_weighted_seasons.append(expected_stats_by_player)
        
        ## Projecting Playing Time Section
        
        partial_df = df.iloc[:, 1:4]
        partial_df = partial_df.set_index(partial_df.columns[1])
        partial_df = partial_df.drop(['Name'], axis=1)

        years_of_data = partial_df.shape[0]
        years_list = all_years_list[(3 - years_of_data):]

        adj_pt_weight_df = playing_time_weight_df.iloc[playing_time_weight_df.index.isin(years_list)]
        
        pt_weighted_df = pd.DataFrame(partial_df.values*adj_pt_weight_df.values, columns=partial_df.columns, index=partial_df.index)
        
        partial_df = df.iloc[:, :3]
        all_player_years_pa = pd.concat([partial_df.reset_index(drop=True), pt_weighted_df.reset_index(drop=True)], axis=1)

        all_player_years_pa = all_player_years_pa.drop(['Name', 'Season'], axis=1)
        
        all_playing_time_weighted_seasons.append(all_player_years_pa)

    all_player_weighted_seasons = pd.concat(all_weighted_seasons, axis=0)
    all_league_weighted_seasons = pd.concat(all_league_weighted_seasons, axis=0)
    all_pt_weighted_seasons = pd.concat(all_playing_time_weighted_seasons, axis=0)

    ## Sum expected stats across all seasons by player
    ## Prorate expected stats to 1200 plate appearances
    ## This will be the regression component used in the player projections

    sum_league_weighted_seasons = all_league_weighted_seasons.groupby(['IDfg', 'weights']).sum()
    sum_league_weighted_seasons = sum_league_weighted_seasons.div(sum_league_weighted_seasons.PA, axis=0) * 1200
    sum_league_weighted_seasons = sum_league_weighted_seasons.reset_index()

    ## Combine expected stats/1200 PA (regression component) table with weighted 3 year stats table

    all_forecast_data = pd.concat([all_player_weighted_seasons, sum_league_weighted_seasons], axis=0)
    all_forecast_data = all_forecast_data.sort_values(by=['IDfg', 'weights']).groupby(['IDfg', 'weights']).sum().round().reset_index()

    ## Combine stats for each player

    sum_all_forecast_data = all_forecast_data.groupby(['IDfg']).sum()

    ## Convert to projected per plate appearance rates

    projected_rates_all_players_no_age = sum_all_forecast_data.div(sum_all_forecast_data.PA, axis=0)

    ## Now apply an age adjustment to the projected rates

    counting_stat_rates = projected_rates_all_players_no_age.iloc[:, 2:]

    # Data frame of player info from most recent season in data set

    player_age_df = player_info_prior_season.iloc[:,:4]
    player_age_df['age_factor'] = player_age_df.apply(lambda row: (29 - (row['Age'] + 1)) * 0.006 if (29 - (row['Age'] + 1)) > 0 else (29 - (row['Age'] + 1)) * 0.003, axis=1)
    player_age_df = player_age_df.set_index(partial_df.columns[0]).drop(['Name', 'Team', 'Age'], axis=1)

    player_age_mult_df = pd.concat([player_age_df] * counting_stat_rates.shape[1], axis=1)
    player_age_mult_df = player_age_mult_df + 1

    age_weighted_rates = pd.DataFrame(counting_stat_rates.values*player_age_mult_df.values, columns=counting_stat_rates.columns, index=counting_stat_rates.index)
    
    ## Create table of 2021 playing time projections

    playing_time_projection = all_pt_weighted_seasons.groupby(['IDfg']).sum() + 200

    ## Multiply projected per PA rates by projected season PA

    full_season_projections_with_playing_time = age_weighted_rates.mul(playing_time_projection.PA, axis=0).round()

    ## Merge full season projections with playing time projections and add at bats column

    season_projections = pd.concat([playing_time_projection.round(), full_season_projections_with_playing_time], axis=1)

    season_projections['AB'] = season_projections.apply(lambda row: row['PA'] - row['BB'] - row['HBP'] - row['SF'], axis=1)

    season_projections['DKp'] = season_projections.apply(lambda batter: draft_king_batters(batter['1B'],
                                                                                batter['2B'],
                                                                                batter['3B'],
                                                                                batter['HR'],
                                                                                batter['RBI'],
                                                                                batter['R'],
                                                                                batter['BB'],
                                                                                batter['HBP'],
                                                                                batter['SB']),axis=1)

    season_projections['Best Ball'] = season_projections.apply(lambda batter: best_ball_batters(batter['AB'],
                                                                                           (batter['1B'] + batter['2B'] + batter['3B'] + batter['HR']),
                                                                                           batter['HR'],
                                                                                           batter['R'],
                                                                                           batter['RBI'],
                                                                                           batter['SB']), axis=1)

    season_projections['AVG'] = season_projections.apply(lambda batter: batting_average_batters(batter['AB'],
                                                                                            batter['1B'],
                                                                                                batter['2B'],
                                                                                                batter['3B'],
                                                                                                batter['HR']), axis=1)

    season_projections['OBP'] = season_projections.apply(lambda batter: onbase_percentage_batters(batter['PA'],
                                                                                            batter['1B'],
                                                                                                batter['2B'],
                                                                                                batter['3B'],
                                                                                                batter['HR'],
                                                                                                batter['BB'],
                                                                                                batter['HBP']), axis=1)

    season_projections['SLG'] = season_projections.apply(lambda batter: slugging_percentage_batters(batter['AB'],
                                                                                            batter['1B'],
                                                                                                batter['2B'],
                                                                                                batter['3B'],
                                                                                                batter['HR']), axis=1)

    ## Re arrange columns
    season_projections = season_projections[['PA', 'AB', 'R', '1B', '2B', '3B', 'HR', 'RBI', 'BB', 'SO', 'HBP', 'SF', 'SB', 'AVG', 'OBP', 'SLG', 'DKp', 'Best Ball']].reset_index()

    ## Convert Draft Kings point projection to a per plate appearance projection

    full_season_projections = pd.merge(player_info_prior_season, season_projections, on='IDfg')
    full_season_projections['Season'] = year
    full_season_projections.to_csv('full_season_projections_' + str(year) + '.csv', index=False)

    per_pa_projection = season_projections
    per_pa_projection.index = per_pa_projection.IDfg
    per_pa_projection = per_pa_projection.div(per_pa_projection.PA, axis=0)[['DKp']].reset_index().round(3)
    points_per_pa_projection = pd.merge(player_info_prior_season, per_pa_projection, on='IDfg')
    points_per_pa_projection = points_per_pa_projection.rename(columns={'DKp': 'DKp/pa'})
    points_per_pa_projection.to_csv('DK_points_per_pa_proj_' + str(year) + '.csv', index=False)


    
    

    





# %%
