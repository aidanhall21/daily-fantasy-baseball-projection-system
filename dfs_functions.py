import pandas as pd
import os
import numpy as np
import math
import requests
from statistics import mean
from pulp import *

def get_batter_salaries(date):
    response = requests.get(f'https://api.sportsdata.io/api/mlb/fantasy/json/DfsSlatesByDate/{date}', headers={'Ocp-Apim-Subscription-Key': '6fcab751d8594ce9909283dcdc522d24'})
    games = response.json()
    df_slates = pd.json_normalize(games)
    df_slates = df_slates[['SlateID', 'Operator', 'OperatorSlateID', 'OperatorName', 'NumberOfGames', 'OperatorGameType', 'SalaryCap']]
    df_player_sal = pd.json_normalize(games, record_path =['DfsSlatePlayers'])
    df_player_sal = df_player_sal[(df_player_sal['OperatorPosition'] != 'SP') & (df_player_sal['OperatorPosition'] != 'RP') & (df_player_sal['OperatorPosition'] != 'P')].reset_index(drop=True)
    df_player_sal = df_player_sal.merge(df_slates, how='left', on='SlateID')

    players = list(df_player_sal.PlayerID.unique())

    return df_player_sal, players

def get_current_season_game_logs_batters(date):
    
    season = date[:4]
    cwd = os.getcwd()
    os.chdir(cwd + '/daily-downloads/Fantasy.2019-2022' + date)

    ## Currently using all past games we have access to for park_adjusted_sals_with_vegas_linesing
    ## In practice you'd just need the current season data file
    game_stats = pd.read_csv(f'PlayerGame.{season}.csv')

    # Select only regular season data (should only be needed on backpark_adjusted_sals_with_vegas_linesing)
    game_stats = game_stats.loc[game_stats.SeasonType == 1].reset_index(drop=True)

    # Select only batter data
    game_stats = game_stats.loc[game_stats.PositionCategory != 'P'].reset_index(drop=True)

    # Calculate plate appearances by game
    game_stats['PlateAppearances'] = game_stats.apply(lambda row: row['AtBats'] + row['Walks'] + row['HitByPitch'] + row['Sacrifices'], axis=1)

    # Just changes name of dataframe
    #data = game_stats.sort_values(['PlayerID', 'Day'], ascending=True).reset_index(drop=True)

    # For park_adjusted_sals_with_vegas_linesing only, need to select only games from prior to the request date
    #data['Day'] = data['Day'].astype('datetime64[ns]')
    #data['Day'] = data['Day'].dt.date
    #date_object = datetime.strptime(date, '%Y-%b-%d').date()

    #data = data[data['Day'] < date_object].reset_index(drop=True)

    game_stats.rename(columns = {'Runs': 'R', 'Singles': 'S', 'Doubles': 'D', 'Triples': 'T', 'HomeRuns': 'HR', 'AtBats': 'AB', 'Walks':'BB', 'RunsBattedIn': 'RBI', 'PlateAppearances': 'PA', 'Hits': 'H', 'HitByPitch': 'HP', 'StolenBases': 'SB', 'CaughtStealing': 'CS', 'Strikeouts': 'SO'},  
            inplace = True)
    
    os.chdir('../..')

    return game_stats

# Retrieves already calculated "marcels" projections
# which are full season projections calculated prior to the 
# start of the specified season
# These act as our baseline assumption of talent level for each player 
def get_marcels_batters(date):

    season = date[:4]

    cwd = os.getcwd()
    os.chdir(cwd + '/BaselineProjections')
    marcels = pd.read_csv(f'marcel_batters_{season}.csv')
    marcels.rename(columns = {'rel': 'Reliability'}, inplace = True)
    os.chdir('..')

    return marcels

def create_stabilization_dict_hitters(dataframe, rel_dict):

    player_dict = {}
    if dataframe.shape[0] == 0: pass # need to include something for the first day of the season
    else:
        player_id_list = list(dataframe.index.values)
    for player in player_id_list:
        player_dict[player] = {}
        for stat in ['PA', 'S', 'D', 'T', 'HR', 'BB', 'HP', 'SB', 'CS', 'SO']:
            value = dataframe.loc[player, stat]
            player_dict[player][stat] = value

        PA = player_dict[player]['PA']
        BB = player_dict[player]['BB']
        HBP = player_dict[player]['HP']
        HR = player_dict[player]['HR']
        S = player_dict[player]['S']
        D = player_dict[player]['D']
        T = player_dict[player]['T']
        CS = player_dict[player]['CS']
        SO = player_dict[player]['SO']

        rel = rel_dict[player]
        rel_fact = (2.2 ** rel) / 2

        # These values represent how much influence a players performance in the current season
        # will have on adjusting our prior estimates of a players per PA talent level
        player_dict[player]['BB_s'] = PA / (PA + (120 * rel_fact))
        player_dict[player]['HBP_s'] = PA / (PA + (240 * rel_fact))
        player_dict[player]['S_s'] = PA / (PA + (290 * rel_fact))
        player_dict[player]['D_s'] = (D + T) / ((D + T) + (48 * rel_fact))
        player_dict[player]['T_s'] = (D + T) / ((D + T) + (48 * rel_fact))
        player_dict[player]['HR_s'] = PA / (PA + (170 * rel_fact))
        player_dict[player]['SBA_s'] = (S + BB + HBP) / ((S + BB + HBP) + (39 * rel_fact))
        player_dict[player]['SO_s'] = PA / (PA + (60 * rel_fact))

    return player_dict

def create_per_pa_marcels_rates_hitters(game_logs_file, marcels_file):
    data_ID = game_logs_file[['PlayerID', 'Name']].drop_duplicates()
    marcels = marcels_file.drop_duplicates(subset=['Name'], keep='first')
    marcels = marcels.merge(data_ID, how='left', on='Name')
    marcels = marcels[marcels['PlayerID'].notna()]
    marcels = marcels.set_index('PlayerID')
    rel_columns = marcels.columns.to_list()[12:30]
    marcels = marcels[rel_columns]
    marcels = marcels.div(marcels.PA, axis=0)
    marcels['S'] = marcels.apply(lambda row: row['H'] - row['D'] - row['T'] - row['HR'], axis=1)
    marcel_players = marcels.index.to_list()
    marcels_dict = marcels.to_dict('index')

    return marcels_dict, marcel_players

def get_average_stats_by_position(date, game_logs, pos_group):

    if pos_group == 'hitters':

        ## Average all stats by position

        average_stats_by_position = game_logs[['Position', 'PA', 'S', 'D', 'T', 'HR', 'BB', 'HP', 'SB', 'CS', 'SO']].reset_index(drop=True).groupby(['Position']).sum()

        ## DH and PH stats get lumped into 1B position
        average_stats_by_position.loc['1B'] = average_stats_by_position.loc[['1B', 'DH', 'PH']].sum()

        ## DFS sites only use the OF position, so LF, RF, and CF get lumped in together
        average_stats_by_position.loc['OF'] = average_stats_by_position.loc[['LF', 'RF', 'CF']].sum()
        average_stats_by_position = average_stats_by_position.drop(['DH', 'PH', 'PR', 'LF', 'RF', 'CF'])

        prior_season_stats = get_prior_season_stats(date)
        prior_season_stats = prior_season_stats.loc[prior_season_stats.SeasonType == 1].reset_index(drop=True)
        prior_season_stats = prior_season_stats.loc[prior_season_stats.PositionCategory != 'P'].reset_index(drop=True)
        prior_season_stats['PlateAppearances'] = prior_season_stats.apply(lambda row: row['AtBats'] + row['Walks'] + row['HitByPitch'] + row['Sacrifices'], axis=1)
        prior_season_stats.rename(columns = {'Runs': 'R', 'Singles': 'S', 'Doubles': 'D', 'Triples': 'T', 'HomeRuns': 'HR', 'AtBats': 'AB', 'Walks':'BB', 'RunsBattedIn': 'RBI', 'PlateAppearances': 'PA', 'Hits': 'H', 'HitByPitch': 'HP', 'StolenBases': 'SB', 'CaughtStealing': 'CS', 'Strikeouts': 'SO'},  
                inplace = True)
        
        prior_season_league_stats = prior_season_stats[['Position', 'PA', 'S', 'D', 'T', 'HR', 'BB', 'HP', 'SB', 'CS', 'SO']].reset_index(drop=True).groupby(['Position']).sum()
        prior_season_league_stats.loc['1B'] = prior_season_league_stats.loc[['1B', 'DH']].sum()
        prior_season_league_stats.loc['OF'] = prior_season_league_stats.loc[['LF', 'RF', 'CF']].sum()
        prior_season_league_stats = prior_season_league_stats.drop(['DH', 'LF', 'RF', 'CF'])
        prior_season_league_stats = prior_season_league_stats.div(prior_season_league_stats.PA, axis=0)

        positions = ['1B', '2B', '3B', 'SS', 'C', 'OF']

        for pos in positions:
            total = prior_season_league_stats.loc[pos] * 2000 + average_stats_by_position.loc[pos]
            new_row = total.divide(total.PA)
            average_stats_by_position.loc[pos] = new_row

        return average_stats_by_position

    elif pos_group == 'pitchers':

        average_stats_by_position = game_logs[['Started', 'W', 'TotalOutsPitched', 'ER', 'BB', 'SO', 'HR', 'H', 'H-HR']].reset_index(drop=True).groupby(['Started']).sum()

        prior_season_stats = get_prior_season_stats(date)
        prior_season_stats = prior_season_stats.loc[prior_season_stats.SeasonType == 1].reset_index(drop=True)
        prior_season_stats = prior_season_stats.loc[prior_season_stats.PositionCategory == 'P'].reset_index(drop=True)
        prior_season_stats['isPrimaryStarter'] = prior_season_stats.apply(lambda row: 1 if row['Started'] > (row['Games'] - row['Started']) else 0, axis=1)
        prior_season_stats.rename(columns = {'Wins': 'W', 'PitchingEarnedRuns': 'ER', 'PitchingWalks': 'BB', 'PitchingStrikeouts': 'SO', 'PitchingHomeRuns': 'HR', 'PitchingHits': 'H'}, inplace = True)
        prior_season_stats['H-HR'] = prior_season_stats.apply(lambda row: row['H'] - row['HR'], axis=1)

        prior_season_league_stats = prior_season_stats[['isPrimaryStarter', 'W', 'TotalOutsPitched', 'ER', 'BB', 'SO', 'HR', 'H', 'H-HR']].reset_index(drop=True).groupby(['isPrimaryStarter']).sum()
        prior_season_league_stats = prior_season_league_stats.div(prior_season_league_stats.TotalOutsPitched, axis=0)

        positions = [0, 1]

        for pos in positions:
            total = prior_season_league_stats.loc[pos] * 15000 + average_stats_by_position.loc[pos]
            new_row = total.divide(total.TotalOutsPitched)
            average_stats_by_position.loc[pos] = new_row

        return average_stats_by_position

    else:

        return

    
def create_blended_projections_hitters(players, marcel_players, player_dict, marcels_dict, df_player_sal, average_stats_by_position):
    ## Different methods based on availability of pre season projections

    player_projs_dict = {}

    for player in players:
        
        if player in marcel_players: # if they have a marcels projection
        
            new_player_dict = {}
            stat_list = ['S', 'D', 'T', 'HR', 'BB', 'HP', 'SO', 'SB']
            stab_list = ['S_s', 'D_s', 'T_s', 'HR_s', 'BB_s', 'HBP_s', 'SO_s', 'SBA_s']
            i = 0

            if player in player_dict:
                
                player_proj = player_dict[player]
                
            else: # If they do not have games (3)
                
                for stat in stat_list:
                    new_player_dict[stat] = marcels_dict[player][stat]
                    
                player_projs_dict[player] = new_player_dict
                
            # both games and marcel (1)

            PA = player_proj['PA']
            
            if PA == 0:
                
                for stat in stat_list:
                    new_player_dict[stat] = marcels_dict[player][stat]
                    
                player_projs_dict[player] = new_player_dict                

            for stat in stat_list:

                if stat == 'SB':

                    opps = player_proj['S'] + player_proj['HP'] + player_proj['BB']
                    sba_exp = (marcels_dict[player][stat] + marcels_dict[player]['CS']) / (marcels_dict[player]['S'] + marcels_dict[player]['BB'] + marcels_dict[player]['HP'])
                    if opps == 0:
                        sba_act = 0
                    else:
                        sba_act = (player_proj[stat] + player_proj['CS']) / opps
                    sba_blend = (sba_act * player_proj['SBA_s']) + (sba_exp * (1 - player_proj['SBA_s']))
                    succ_rate_proj = marcels_dict[player][stat] / (marcels_dict[player][stat] + marcels_dict[player]['CS'])

                else:

                    stat_exp = marcels_dict[player][stat] * PA
                    stat_act = player_proj[stat]
                    stat_blend = (stat_act * player_proj[stab_list[i]]) + (stat_exp * (1 - player_proj[stab_list[i]]))

                    new_player_dict[stat_list[i]] = stat_blend / PA

                    i += 1

            new_player_dict['SB'] = sba_blend * succ_rate_proj * (new_player_dict['S'] + new_player_dict['BB'] - new_player_dict['HP'])

            player_projs_dict[player] = new_player_dict
        
        else: 
            
            if player in player_dict: # Check if they've played games
                # No marcels, but games (2)
                # position average acts as default marcels projections
                player_proj = player_dict[player]
                
                eff_pos = df_player_sal.loc[df_player_sal['PlayerID'] == player, 'EffectivePosition'].iloc[0]
                if eff_pos == 'DH':
                    eff_pos = '1B'
                proj_by_position = average_stats_by_position.loc[eff_pos]
                proj_by_pos_dict = proj_by_position.to_dict()
                
                new_player_dict = {}
                stat_list = ['S', 'D', 'T', 'HR', 'BB', 'HP', 'SO', 'SB']
                stab_list = ['S_s', 'D_s', 'T_s', 'HR_s', 'BB_s', 'HBP_s', 'SO_s', 'SBA_s']
                i = 0
                
                PA = player_proj['PA']
            
                if PA == 0:

                    for stat in stat_list:
                        new_player_dict[stat] = proj_by_pos_dict[stat]

                    player_projs_dict[player] = new_player_dict

                for stat in stat_list:

                    if stat == 'SB':

                        opps = player_proj['S'] + player_proj['HP'] + player_proj['BB']
                        sba_exp = (proj_by_pos_dict[stat] + proj_by_pos_dict['CS']) / (proj_by_pos_dict['S'] + proj_by_pos_dict['BB'] + proj_by_pos_dict['HP'])
                        if opps == 0:
                            sba_act = 0
                        else:
                            sba_act = (player_proj[stat] + player_proj['CS']) / opps
                        sba_blend = (sba_act * player_proj['SBA_s']) + (sba_exp * (1 - player_proj['SBA_s']))
                        succ_rate_proj = proj_by_pos_dict[stat] / (proj_by_pos_dict[stat] + proj_by_pos_dict['CS'])

                    else:

                        stat_exp = proj_by_pos_dict[stat] * PA
                        stat_act = player_proj[stat]
                        stat_blend = (stat_act * player_proj[stab_list[i]]) + (stat_exp * (1 - player_proj[stab_list[i]]))

                        new_player_dict[stat_list[i]] = stat_blend / PA

                        i += 1

                new_player_dict['SB'] = sba_blend * succ_rate_proj * (new_player_dict['S'] + new_player_dict['BB'] - new_player_dict['HP'])

                player_projs_dict[player] = new_player_dict
                
            else: # Neither games nor marcels (4)
            
                # Find effective position

                eff_pos = df_player_sal.loc[df_player_sal['PlayerID'] == player, 'EffectivePosition'].iloc[0]
                proj_by_position = average_stats_by_position.loc[eff_pos]
                proj_by_pos_dict = proj_by_position.to_dict()
                player_projs_dict[player] = proj_by_pos_dict

    return player_projs_dict

def get_vegas_lines(date, player_salaries_df):

    response = requests.get(f'https://api.sportsdata.io/api/mlb/fantasy/json/DfsSlatesByDate/{date}', headers={'Ocp-Apim-Subscription-Key': '6fcab751d8594ce9909283dcdc522d24'})
    games = response.json()
    df_games = pd.json_normalize(games, record_path =['DfsSlateGames'])
    df_games = df_games[['SlateGameID', 'GameID', 'OperatorGameID', 'Game.Season', 'Game.Day', 'Game.AwayTeam', 'Game.HomeTeam', 'Game.AwayTeamID', 'Game.HomeTeamID', 'Game.StadiumID', 'Game.AwayTeamProbablePitcherID', 'Game.HomeTeamProbablePitcherID', 'Game.PointSpread', 'Game.OverUnder', 'Game.AwayTeamMoneyLine', 'Game.HomeTeamMoneyLine']]
    result_df = player_salaries_df.merge(df_games, how='left', on = ['SlateGameID'])

    starting_pitchers = list(set(list(df_games['Game.AwayTeamProbablePitcherID'])).union(set(list(df_games['Game.HomeTeamProbablePitcherID']))))
    starting_pitchers = [x for x in starting_pitchers if str(x) != 'nan']

    result_df['HomeOrAway'] = result_df.apply(lambda row: 'AWAY' if row['Game.AwayTeamID'] == row['TeamID'] else 'HOME', axis=1)
    result_df['PlayerTeamMoneyLine'] = result_df.apply(lambda row: row['Game.AwayTeamMoneyLine'] if row['HomeOrAway'] == 'AWAY' else row['Game.HomeTeamMoneyLine'], axis=1)
    result_df['PlayerTeamPointSpread'] = result_df.apply(lambda row: abs(row['Game.PointSpread']) * -1 if row['PlayerTeamMoneyLine'] < 0 else abs(row['Game.PointSpread']), axis=1)
    result_df['PlayerTeamVegasWinProb'] = result_df.apply(lambda row: 100 / (100 + row['PlayerTeamMoneyLine']) if row['PlayerTeamMoneyLine'] > 0 else row['PlayerTeamMoneyLine'] / (row['PlayerTeamMoneyLine'] - 100), axis=1)
    result_df['PlayerTeamTotal'] = result_df.apply(lambda row: round((row['Game.OverUnder'] / 2) - ((row['PlayerTeamPointSpread'] * (100 / (abs(row['PlayerTeamMoneyLine']) + 100))) / 2), 2), axis=1)

    return result_df, starting_pitchers

def adjust_for_park_factors(sals_with_vegas_lines):
    park_factors = pd.read_csv('ParkFactors.csv')
    park_adjusted_sals_with_vegas_lines = sals_with_vegas_lines.merge(park_factors, how = 'left', on = ['Game.StadiumID'])
    park_adjusted_sals_with_vegas_lines['pS/PA'] = park_adjusted_sals_with_vegas_lines['pS/PA'] * park_adjusted_sals_with_vegas_lines['1B'] / 100
    park_adjusted_sals_with_vegas_lines['pD/PA'] = park_adjusted_sals_with_vegas_lines['pD/PA'] * park_adjusted_sals_with_vegas_lines['2B'] / 100
    park_adjusted_sals_with_vegas_lines['pT/PA'] = park_adjusted_sals_with_vegas_lines['pT/PA'] * park_adjusted_sals_with_vegas_lines['3B'] / 100
    park_adjusted_sals_with_vegas_lines['pHR/PA'] = park_adjusted_sals_with_vegas_lines['pHR/PA'] * park_adjusted_sals_with_vegas_lines['HR'] / 100
    park_adjusted_sals_with_vegas_lines['pBB/PA'] = park_adjusted_sals_with_vegas_lines['pBB/PA'] * park_adjusted_sals_with_vegas_lines['BB'] / 100
    park_adjusted_sals_with_vegas_lines['pSO/PA'] = park_adjusted_sals_with_vegas_lines['pSO/PA'] * park_adjusted_sals_with_vegas_lines['SO'] / 100

    return park_adjusted_sals_with_vegas_lines

def apply_starters_obp(salaries_df):

    starters_obp_data = salaries_df[salaries_df['battingorderposition'].notna()]
    starters_obp_data = starters_obp_data[['PlayerID', 'TeamID', 'pS/PA', 'pD/PA', 'pT/PA', 'pHR/PA', 'pBB/PA', 'pHP/PA']].drop_duplicates(['PlayerID'])
    starters_obp_data = starters_obp_data[['TeamID', 'pS/PA', 'pD/PA', 'pT/PA', 'pHR/PA', 'pBB/PA', 'pHP/PA']].reset_index(drop=True).groupby(['TeamID']).mean()
    starters_obp_data['pOBP'] = starters_obp_data.apply(lambda row: row['pS/PA'] + row['pD/PA'] + row['pT/PA'] + row['pHR/PA'] + row['pBB/PA'] + row['pHP/PA'], axis=1)
    salaries_df['startersOBP'] = salaries_df.apply(lambda row: starters_obp_data.loc[row['TeamID'], 'pOBP'] if row['TeamID'] in list(starters_obp_data.index) else 0, axis=1)

    return salaries_df

def find_lead_hitters_obp(salaries_df):

    temp_df = salaries_df[['PlayerID', 'TeamID', 'battingorderposition', 'pS/PA', 'pD/PA', 'pT/PA', 'pBB/PA', 'pHP/PA']].drop_duplicates(['PlayerID']).dropna(subset=['battingorderposition']).reset_index(drop=True)
    leadOBP_dict = {}

    for index, row in temp_df.iterrows():
        
        teamid = row['TeamID']
        playerid = row['PlayerID']
        b_order = row['battingorderposition']
        lead_hitters = []
        
        if b_order > 3:
            lead_hitters = [b_order - 1, b_order - 2, b_order - 3]
        elif b_order == 3: lead_hitters = [9, 2, 1]
        elif b_order == 2: lead_hitters = [9, 8, 1]
        else:
            lead_hitters = [7, 8, 9]
        
        
        sub_df = temp_df[temp_df['TeamID'] == teamid].reset_index(drop=True)
        boolean_series = sub_df.battingorderposition.isin(lead_hitters)
        filtered_df = sub_df[boolean_series].reset_index(drop=True)
        OB_events = (filtered_df['pS/PA'].sum() + filtered_df['pD/PA'].sum() + filtered_df['pT/PA'].sum() + filtered_df['pBB/PA'].sum() + filtered_df['pHP/PA'].sum()) / filtered_df.shape[0]
        
        #SLG_events = filtered_df.S.sum() + (2 * filtered_df.D.sum()) + (3 * filtered_df['T'].sum()) + (4 * filtered_df.HR.sum())


        leadOBP_dict[playerid] = OB_events

    return leadOBP_dict

def find_trail_hitters_ops(salaries_df):
    ## Could figure out how to handle pitchers in the 9 spot

    temp_df = salaries_df[['PlayerID', 'TeamID', 'battingorderposition', 'pS/PA', 'pD/PA', 'pT/PA', 'pHR/PA', 'pBB/PA', 'pHP/PA', 'pAB/PA']].drop_duplicates(['PlayerID']).dropna(subset=['battingorderposition']).reset_index(drop=True)
    trailOPS_dict = {}

    for index, row in temp_df.iterrows():
        
        teamid = row['TeamID']
        playerid = row['PlayerID']
        b_order = row['battingorderposition']
        lead_hitters = []
        
        if b_order < 7:
            lead_hitters = [b_order + 1, b_order + 2, b_order + 3]
        elif b_order == 7: lead_hitters = [9, 8, 1]
        elif b_order == 8: lead_hitters = [9, 2, 1]
        else:
            lead_hitters = [1, 2, 3]
        
        
        sub_df = temp_df[temp_df['TeamID'] == teamid].reset_index(drop=True)
        boolean_series = sub_df.battingorderposition.isin(lead_hitters)
        filtered_df = sub_df[boolean_series].reset_index(drop=True)
        OB_events = (filtered_df['pS/PA'].sum() + filtered_df['pD/PA'].sum() + filtered_df['pT/PA'].sum() + filtered_df['pHR/PA'].sum() + filtered_df['pBB/PA'].sum() + filtered_df['pHP/PA'].sum()) / filtered_df.shape[0]
        SLG_events = (filtered_df['pS/PA'].sum() + (2 * filtered_df['pD/PA'].sum()) + (3 * filtered_df['pT/PA'].sum()) + (4 * filtered_df['pHR/PA'].sum()) / filtered_df['pAB/PA'].mean()) / filtered_df.shape[0]
        
        OPS = OB_events + SLG_events


        trailOPS_dict[playerid] = OPS

    return trailOPS_dict

def generate_projection_df_hitters(sals_with_batting_order):

    sals_with_batting_order['pBA'] = sals_with_batting_order.apply(lambda row: (row['pS/PA'] + row['pD/PA'] + row['pT/PA'] + row['pHR/PA']) / row['pAB/PA'], axis=1)
    sals_with_batting_order['pRBI/PA'] = sals_with_batting_order.apply(lambda row: -0.065 + (1.3 * row['pHR/PA']) + (0.2 * row['pBA']), axis=1)
    sals_with_batting_order['pR-HR/PA'] = sals_with_batting_order.apply(lambda row: -0.055 + (.245 * row['OBP-HR']), axis=1)
    sals_with_batting_order['pR/PA'] = sals_with_batting_order.apply(lambda row: row['pR-HR/PA'] + row['pHR/PA'], axis=1)
    sals_with_batting_order['pS'] = sals_with_batting_order.apply(lambda row: round(row['pS/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['pD'] = sals_with_batting_order.apply(lambda row: round(row['pD/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['pT'] = sals_with_batting_order.apply(lambda row: round(row['pT/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['pHR'] = sals_with_batting_order.apply(lambda row: round(row['pHR/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['pBB'] = sals_with_batting_order.apply(lambda row: round(row['pBB/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['pHP'] = sals_with_batting_order.apply(lambda row: round(row['pHP/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['pR'] = sals_with_batting_order.apply(lambda row: round(row['pR/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['pRBI'] = sals_with_batting_order.apply(lambda row: round(row['pRBI/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['pSB'] = sals_with_batting_order.apply(lambda row: round(row['pSB/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['pSO'] = sals_with_batting_order.apply(lambda row: round(row['pSO/PA'] * row['pPA'], 2), axis=1)
    sals_with_batting_order['DraftKingsPoints'] = sals_with_batting_order.apply(lambda row: round((3 * row['pS']) + (5 * row['pD']) + (8 * row['pT']) + (10 * row['pHR']) + (2 * row['pRBI']) + (2 * row['pR']) + (2 * row['pBB']) + (2 * row['pHP']) + (5 * row['pSB']), 2), axis=1)
    sals_with_batting_order['FanDuelPoints'] = sals_with_batting_order.apply(lambda row: round((3 * row['pS']) + (6 * row['pD']) + (9 * row['pT']) + (12 * row['pHR']) + (3.5 * row['pRBI']) + (3.2 * row['pR']) + (3 * row['pBB']) + (3 * row['pHP']) + (6 * row['pSB']), 2), axis=1)
    sals_with_batting_order['H_A'] = sals_with_batting_order.apply(lambda row: 'H' if row['Game.HomeTeamID'] == row['TeamID'] else 'A', axis=1)
    sals_with_batting_order['Opponent_ID'] = sals_with_batting_order.apply(lambda row: row['Game.HomeTeamID'] if row['H_A'] == 'A' else row['Game.AwayTeamID'], axis=1)
    projection_df = sals_with_batting_order[['PlayerID','SlateID', 'Operator', 'OperatorPlayerID', 'TeamID', 'OperatorSalary','OperatorGameType', 'SalaryCap', 'OperatorPlayerName', 'OperatorPosition', 'OperatorRosterSlots', 'pPA', 'pR', 'pS', 'pD', 'pT', 'pHR', 'pRBI', 'pBB', 'pHP', 'pSB', 'pSO', 'DraftKingsPoints', 'FanDuelPoints', 'H_A', 'Opponent_ID']].reset_index(drop=True)

    return projection_df

def get_pitcher_salaries(date):
    response = requests.get(f'https://api.sportsdata.io/api/mlb/fantasy/json/DfsSlatesByDate/{date}', headers={'Ocp-Apim-Subscription-Key': '6fcab751d8594ce9909283dcdc522d24'})
    games = response.json()
    df_slates = pd.json_normalize(games)
    df_slates = df_slates[['SlateID', 'Operator', 'OperatorSlateID', 'OperatorName', 'NumberOfGames', 'OperatorGameType', 'SalaryCap']]
    df_player_sal = pd.json_normalize(games, record_path =['DfsSlatePlayers'])
    df_player_sal = df_player_sal[(df_player_sal['OperatorPosition'] == 'SP') | (df_player_sal['OperatorPosition'] == 'RP') | (df_player_sal['OperatorPosition'] == 'P')].reset_index(drop=True)
    df_player_sal = df_player_sal.merge(df_slates, how='left', on='SlateID')

    players = list(df_player_sal.PlayerID.unique())

    return df_player_sal, players


def get_current_season_game_logs_pitchers(date):
    season = date[:4]
    cwd = os.getcwd()
    os.chdir(cwd + '/daily-downloads/Fantasy.2019-2022' + date)

    ## In practice you'd just need the current season data file
    game_stats = pd.read_csv(f'PlayerGame.{season}.csv')

    # Select only regular season data (should only be needed on backpark_adjusted_sals_with_vegas_linesing)
    game_stats = game_stats.loc[game_stats.SeasonType == 1].reset_index(drop=True)

    # Select only pitcher data
    game_stats = game_stats.loc[game_stats.PositionCategory == 'P'].reset_index(drop=True)

    # Just changes name of dataframe
    data = game_stats.sort_values(['PlayerID', 'Day'], ascending=True).reset_index(drop=True)

    # For beta park_adjusted_sals_with_vegas_linesing only, need to select only games from prior to the request date
    #data['Day'] = data['Day'].astype('datetime64[ns]')
    #data['Day'] = data['Day'].dt.date
    #date_object = datetime.strptime(date, '%Y-%b-%d').date()

    #data = data[data['Day'] < date_object].reset_index(drop=True)

    data.rename(columns = {'Wins': 'W', 'PitchingEarnedRuns': 'ER', 'PitchingWalks': 'BB', 'PitchingStrikeouts': 'SO', 'PitchingHomeRuns': 'HR', 'PitchingHits': 'H'}, inplace = True) 
    data['H-HR'] = data.apply(lambda row: row['H'] - row['HR'], axis=1)
    os.chdir('../..')

    return data

def get_league_stats(date):

    season = date[:4]
    cwd = os.getcwd()
    os.chdir(cwd + '/daily-downloads/Fantasy.2019-2022' + date)

    ## Currently using all past games we have access to for park_adjusted_sals_with_vegas_linesing
    ## In practice you'd just need the current season data file
    game_stats = pd.read_csv(f'PlayerGame.{season}.csv')

    # Select only regular season data (should only be needed on backpark_adjusted_sals_with_vegas_linesing)
    league_stats = game_stats.loc[game_stats.SeasonType == 1].reset_index(drop=True)

    # Select only batter data
    league_stats = league_stats.loc[league_stats.PositionCategory != 'P'].reset_index(drop=True)

    #league_stats['Day'] = league_stats['Day'].astype('datetime64[ns]')
    #league_stats['Day'] = league_stats['Day'].dt.date
    #date_object = datetime.strptime(date, '%Y-%b-%d').date()

    #league_stats = league_stats[league_stats['Day'] < date_object].reset_index(drop=True)
    os.chdir('../..')

    return league_stats

def get_marcels_pitchers(date):

    season = date[:4]

    cwd = os.getcwd()
    os.chdir(cwd + '/BaselineProjections')
    marcels = pd.read_csv(f'marcel_pitchers_{season}.csv')

    marcels['TotalOutsPitched'] = marcels.apply(lambda row: row['IP'] * 3, axis=1)
    marcels.rename(columns = {'rel': 'Reliability'}, inplace = True)
    os.chdir('..')

    return marcels

def create_stabilization_dict_pitchers(sum_data, reliability_dict):
    ## Create dictionary of current season total stats and stabilization factors for each player
    ## Separate out HR from non HR

    player_dict = {}
    if sum_data.shape[0] == 0: pass # need to include something for the first day of the season
    else:
        player_id_list = list(sum_data.index.values)
    for player in player_id_list:
        player_dict[player] = {}
        for stat in ['TotalOutsPitched', 'ER', 'BB', 'SO', 'H', 'HR', 'H-HR']:
            value = sum_data.loc[player, stat]
            player_dict[player][stat] = value

        Outs = player_dict[player]['TotalOutsPitched']
        ER = player_dict[player]['ER']
        BB = player_dict[player]['BB']
        K = player_dict[player]['SO']
        H = player_dict[player]['H']
        HR = player_dict[player]['HR']
        H_HR = player_dict[player]['H-HR']
        
        PA_est = Outs + H + BB

        rel = reliability_dict[player]
        rel_fact = (2.2 ** rel) / 2

        player_dict[player]['SO_s'] = PA_est / (PA_est + (126 * rel_fact))
        player_dict[player]['BB_s'] = PA_est / (PA_est + (303 * rel_fact))
        player_dict[player]['H-HR_s'] = (PA_est - BB - HR - K) / ((PA_est - BB - HR - K) + (3729 * rel_fact))
        player_dict[player]['HR_s'] = (PA_est - BB - K) / (((PA_est - BB - K) + (1271 * rel_fact)))


    return player_dict

def create_per_pa_marcels_rates_pitchers(game_logs_file, marcels_file):
    data_ID = game_logs_file[['PlayerID', 'Name']].drop_duplicates()
    marcels = marcels_file.merge(data_ID, how='left', on='Name')
    marcels = marcels[marcels['PlayerID'].notna()]
    marcels = marcels.set_index('PlayerID')
    rel_columns = marcels.columns.to_list()[8:]
    marcels = marcels[rel_columns]
    marcels = marcels.div(marcels.TotalOutsPitched, axis=0)
    marcels['H-HR'] = marcels.apply(lambda row: row['H'] - row['HR'], axis=1)
    marcel_players = marcels.index.to_list()
    marcels_dict = marcels.to_dict('index')

    return marcels_dict, marcel_players

def create_blended_projections_pitchers(players, marcel_players, player_dict, marcels_dict, sum_data, average_stats_by_position):
    ## Create blended projections for the request date
    ## Different methods based on availability of pre season projections

    player_projs_dict = {}

    for player in players:
        
        if player in marcel_players:
        
            new_player_dict = {}
            stat_list = ['SO', 'BB', 'HR', 'H-HR']
            stab_list = ['SO_s', 'BB_s', 'HR_s', 'H-HR_s']
            i = 0

            try:
                player_proj = player_dict[player]
            except:
                
                for stat in stat_list:
                    new_player_dict[stat] = marcels_dict[player][stat]
                    
                player_projs_dict[player] = new_player_dict
                continue


            Outs = player_proj['TotalOutsPitched']
            
            if Outs == 0:
                
                for stat in stat_list:
                    new_player_dict[stat] = marcels_dict[player][stat]
                    
                player_projs_dict[player] = new_player_dict
                continue
                

            for stat in stat_list:

                stat_exp = marcels_dict[player][stat] * Outs
                stat_act = player_proj[stat]
                stat_blend = (stat_act * player_proj[stab_list[i]]) + (stat_exp * (1 - player_proj[stab_list[i]]))

                new_player_dict[stat_list[i]] = stat_blend / Outs

                i += 1

            player_projs_dict[player] = new_player_dict
        
        else:
            
            if player in player_dict: # Check if they've played games
                # No marcels, but games (2)
                # position average acts as default marcels projections
                player_proj = player_dict[player]
                
                starts = sum_data.loc[player, 'Started']
                games = sum_data.loc[player, 'Games']
                
                if starts > (games - starts):
                    eff_pos = 1
                else:
                    eff_pos = 0
                
                proj_by_position = average_stats_by_position.loc[eff_pos]
                proj_by_position = proj_by_position.divide(proj_by_position.TotalOutsPitched)
                proj_by_pos_dict = proj_by_position.to_dict()
                
                new_player_dict = {}
                stat_list = ['SO', 'BB', 'HR', 'H-HR']
                stab_list = ['SO_s', 'BB_s', 'HR_s', 'H-HR_s']
                i = 0
                
                Outs = player_proj['TotalOutsPitched']

                if Outs == 0:

                    for stat in stat_list:
                        new_player_dict[stat] = proj_by_pos_dict[stat]

                    player_projs_dict[player] = new_player_dict
                    continue


                for stat in stat_list:

                    stat_exp = proj_by_pos_dict[stat] * Outs
                    stat_act = player_proj[stat]
                    stat_blend = (stat_act * player_proj[stab_list[i]]) + (stat_exp * (1 - player_proj[stab_list[i]]))

                    new_player_dict[stat_list[i]] = stat_blend / Outs

                    i += 1

                player_projs_dict[player] = new_player_dict
                
            else:

                # Find effective position

                eff_pos = 0
                proj_by_position = average_stats_by_position.loc[eff_pos]
                proj_by_position = proj_by_position.divide(proj_by_position.TotalOutsPitched)
                proj_by_pos_dict = proj_by_position.to_dict()
                player_projs_dict[player] = proj_by_pos_dict       
            


    return player_projs_dict

def get_prior_season_game_logs(date):

    season = str(int(date[:4]) - 1)

    # based on current file structure and the order of events in the master_projections formulas
    cwd = os.getcwd()
    os.chdir(cwd + '/daily-downloads/Fantasy.2019-2022' + date)
    game_stats_prior = pd.read_csv(f'PlayerGame.{season}.csv')
    # Select only regular season data (should only be needed on backpark_adjusted_sals_with_vegas_linesing)
    game_stats_prior = game_stats_prior.loc[game_stats_prior.SeasonType == 1].reset_index(drop=True)
    # Select only pitcher data
    game_stats_prior = game_stats_prior.loc[game_stats_prior.PositionCategory == 'P'].reset_index(drop=True)
    # Select only starts
    game_stats_prior = game_stats_prior.loc[game_stats_prior.Started == 1].reset_index(drop=True)
    game_stats_prior['ER/out'] = game_stats_prior.apply(lambda row: row['PitchingEarnedRuns'] / row['TotalOutsPitched'] if row['TotalOutsPitched'] > 0 else 0, axis=1)

    os.chdir('../..')

    return game_stats_prior

def generate_starting_pitcher_projections(list_of_starters, result_df, prior_year_ind_pitcher_dist, prior_year_league_innings_dist, current_year_ind_pitcher_dist, current_year_league_innings_dist, current_year_outs, weighted_league_innings_dist_mean, weighted_league_innings_dist_std, current_year_starts_vs_team, FIP_constant):
    all_starters = {}

    for starter in list_of_starters:
        try:
            starter_team = result_df.loc[result_df.PlayerID == starter,'TeamID'].reset_index(drop=True)[0]
        except: continue
        home_team = result_df.loc[result_df.PlayerID == starter,'Game.HomeTeamID'].reset_index(drop=True)[0]
        away_team = result_df.loc[result_df.PlayerID == starter,'Game.AwayTeamID'].reset_index(drop=True)[0]
        if starter_team == home_team:
            opponent_id = away_team
        else:
            opponent_id = home_team
            
        starter_team_w_pct = result_df.loc[result_df.PlayerID == starter,'PlayerTeamVegasWinProb'].reset_index(drop=True)[0]
        
        if math.isnan(starter_team_w_pct):
            starter_team_w_pct = 0.5
        else: pass
            
        try:    
            mean_vs_team = current_year_starts_vs_team.loc[opponent_id]['mean']
            total_outs_vs_team = current_year_starts_vs_team.loc[opponent_id]['sum']
            weighted_outs = total_outs_vs_team / 10
        except:
            mean_vs_team = weighted_league_innings_dist_mean
            weighted_outs = 0
            
        if starter in current_year_ind_pitcher_dist.index:
            current_year_pitcher_outs = current_year_ind_pitcher_dist.loc[starter]['sum']
            if starter in prior_year_ind_pitcher_dist.index:
                prior_year_pitcher_outs = prior_year_ind_pitcher_dist.loc[starter]['sum']
                total_outs = current_year_pitcher_outs + prior_year_pitcher_outs
                mean_of_starter = ((current_year_ind_pitcher_dist.loc[starter]['mean'] * current_year_pitcher_outs) + (prior_year_ind_pitcher_dist.loc[starter]['mean'] * prior_year_pitcher_outs)) / total_outs
            else:
                mean_of_starter = ((current_year_ind_pitcher_dist.loc[starter]['mean'] * current_year_pitcher_outs) + (weighted_league_innings_dist_mean * 100)) / (current_year_pitcher_outs + 100)
                total_outs = current_year_pitcher_outs
        else:
            if starter in prior_year_ind_pitcher_dist.index:
                prior_year_pitcher_outs = prior_year_ind_pitcher_dist.loc[starter]['sum']
                mean_of_starter = ((prior_year_ind_pitcher_dist.loc[starter]['mean'] * prior_year_pitcher_outs) + (weighted_league_innings_dist_mean * 100)) / (prior_year_pitcher_outs + 100)
                total_outs = prior_year_pitcher_outs
            else:
                mean_of_starter = weighted_league_innings_dist_mean
                total_outs = 0
            
        mean_of_league = weighted_league_innings_dist_mean
        
        combined_mean = (((mean_vs_team * weighted_outs) + (mean_of_starter * total_outs) + (mean_of_league * 100)) / (weighted_outs + total_outs + 100))

        try:
            var_vs_team = current_year_starts_vs_team.loc[opponent_id]['std'] ** 2
            total_outs_vs_team = current_year_starts_vs_team.loc[opponent_id]['sum']
            weighted_outs = total_outs_vs_team / 10
        except:
            var_vs_team = weighted_league_innings_dist_std ** 2
            weighed_outs = 0
        
        if starter in current_year_ind_pitcher_dist.index:
            current_year_pitcher_outs = current_year_ind_pitcher_dist.loc[starter]['sum']
            if starter in prior_year_ind_pitcher_dist.index:
                prior_year_pitcher_outs = prior_year_ind_pitcher_dist.loc[starter]['sum']
                total_outs = current_year_pitcher_outs + prior_year_pitcher_outs
                var_of_starter = (((current_year_ind_pitcher_dist.loc[starter]['std'] ** 2) * current_year_pitcher_outs) + ((prior_year_ind_pitcher_dist.loc[starter]['std'] ** 2) * prior_year_pitcher_outs)) / total_outs
            else:
                var_of_starter = (((current_year_ind_pitcher_dist.loc[starter]['std'] ** 2) * current_year_pitcher_outs) + ((weighted_league_innings_dist_std ** 2) * 100)) / (current_year_pitcher_outs + 100)
                total_outs = current_year_pitcher_outs 
        else:
            if starter in prior_year_ind_pitcher_dist.index:
                prior_year_pitcher_outs = prior_year_ind_pitcher_dist.loc[starter]['sum']
                var_of_starter = (((prior_year_ind_pitcher_dist.loc[starter]['std'] ** 2) * prior_year_pitcher_outs) + ((weighted_league_innings_dist_std ** 2) * 100)) / (prior_year_pitcher_outs + 100)
                total_outs = prior_year_pitcher_outs
            else:
                var_of_starter = weighted_league_innings_dist_std ** 2
                total_outs = 0

        var_of_league = weighted_league_innings_dist_std ** 2
        
        
        total_var_outs = weighted_outs + total_outs + 100
        combined_var = ((((weighted_outs / total_var_outs) ** 2) * var_vs_team) + (((total_outs / total_var_outs) ** 2) * var_of_starter) + (((100 / total_var_outs) ** 2) * var_of_league))
        combined_std = np.sqrt(combined_var)
        
        s = np.random.normal(combined_mean, combined_std, 1000)
        
        k_per_out = result_df.loc[result_df.PlayerID == starter, 'pSO/Out'].reset_index(drop=True)[0]
        bb_per_out = result_df.loc[result_df.PlayerID == starter, 'pBB/Out'].reset_index(drop=True)[0]
        hr_per_out = result_df.loc[result_df.PlayerID == starter, 'pHR/Out'].reset_index(drop=True)[0]
        h_hr_per_out = result_df.loc[result_df.PlayerID == starter, 'pH-HR/Out'].reset_index(drop=True)[0]
        hbp_per_out = result_df.loc[result_df.PlayerID == starter, 'pHBP/Out'].reset_index(drop=True)[0]

        ks = []
        bbs = []
        hrs = []
        h_min_hr = []
        hbps = []
        ers = []
        qs = []
        over_5 = []
        ips = []

        for i in range(len(s)):
            sim_ks = s[i] * k_per_out
            sim_bbs = s[i] * bb_per_out
            sim_hrs = s[i] * hr_per_out
            sim_hits = s[i] * h_hr_per_out
            sim_hbp = s[i] * hbp_per_out
            sim_ip = s[i] / 3
            
            ips.append(sim_ip)

            sim_fip = (((13 * sim_hrs) + (3 * (sim_bbs + sim_hbp)) - (2 * sim_ks)) / sim_ip) + FIP_constant
            fip_total_er = (sim_fip / 9) * sim_ip
            sim_er_total = np.random.normal(fip_total_er, 1) 
            
            if (sim_ip >= 6) & (sim_er_total <= 3):
                qs.append(1)
            else:
                qs.append(0)

            if sim_ip >= 5:
                over_5.append(1)
            else:
                over_5.append(0)

            ks.append(sim_ks)
            bbs.append(sim_bbs)
            hrs.append(sim_hrs)
            h_min_hr.append(sim_hits)
            hbps.append(sim_hbp)
            ers.append(sim_er_total)
        

        starter_dict = {}
        
        starter_dict['pIP'] = round(mean(ips), 2)
        starter_dict['pK'] = round(mean(ks), 2)
        starter_dict['pBB'] = round(mean(bbs), 2)
        starter_dict['pHR'] = round(mean(hrs), 2)
        starter_dict['pH'] = round(mean(hrs) + mean(h_min_hr), 2)
        starter_dict['pHBP'] = round(mean(hbps), 2)
        starter_dict['pQS'] = round(mean(qs), 2)
        starter_dict['pER'] = round(mean(ers), 2)
        starter_dict['pW'] = round(mean(over_5) * starter_team_w_pct, 2)
        
        all_starters[starter] = starter_dict

    return all_starters

def generate_projection_df_pitchers(sals_df, all_starters_dict, FIP_constant):

    sals_df['pIP'] = sals_df.apply(lambda row: all_starters_dict[row['PlayerID']]['pIP'] if row['PlayerID'] in all_starters_dict else 1, axis=1)
    sals_df['pW'] = sals_df.apply(lambda row: all_starters_dict[row['PlayerID']]['pW'] if row['PlayerID'] in all_starters_dict else 0, axis=1)
    sals_df['pQS'] = sals_df.apply(lambda row: all_starters_dict[row['PlayerID']]['pQS'] if row['PlayerID'] in all_starters_dict else 0, axis=1)
    sals_df['pK'] = sals_df.apply(lambda row: all_starters_dict[row['PlayerID']]['pK'] if row['PlayerID'] in all_starters_dict else row['pSO/Out'] * 3, axis=1)
    sals_df['pBB'] = sals_df.apply(lambda row: all_starters_dict[row['PlayerID']]['pBB'] if row['PlayerID'] in all_starters_dict else row['pBB/Out'] * 3, axis=1)
    sals_df['pHR'] = sals_df.apply(lambda row: all_starters_dict[row['PlayerID']]['pHR'] if row['PlayerID'] in all_starters_dict else row['pHR/Out'] * 3, axis=1)
    sals_df['pH'] = sals_df.apply(lambda row: all_starters_dict[row['PlayerID']]['pH'] if row['PlayerID'] in all_starters_dict else (row['pH-HR/Out'] + row['pHR/Out']) * 3, axis=1)
    sals_df['pHBP'] = sals_df.apply(lambda row: all_starters_dict[row['PlayerID']]['pHBP'] if row['PlayerID'] in all_starters_dict else row['pHBP/Out'] * 3, axis=1)
    sals_df['pER'] = sals_df.apply(lambda row: all_starters_dict[row['PlayerID']]['pER'] if row['PlayerID'] in all_starters_dict else round(((((13 * row['pHR']) + (3 * (row['pBB'] + row['pHBP'])) - (2 * row['pK'])) / 1) + FIP_constant) / 9, 2), axis=1)
    sals_df['pBF'] = sals_df.apply(lambda row: 3 * row['pIP'] + row['pBB'] + row['pH'] + row['pHBP'], axis=1)
    sals_df['DraftKingsPoints'] = sals_df.apply(lambda row: round(row['pIP'] * 2.25 + row['pK'] * 2 + row['pW'] * 4 + row['pER'] * -2 + row['pH'] * -0.6 + row['pBB'] * -0.6 + row['pHBP'] * -0.6, 2), axis=1)
    sals_df['FanDuelPoints'] = sals_df.apply(lambda row: round(row['pW'] * 6 + row['pQS'] * 4 + row['pER'] * -3 + row['pK'] * 3 + row['pIP'] * 3, 2), axis=1)

    projection_df = sals_df[['PlayerID','SlateID', 'Operator', 'OperatorPlayerID', 'TeamID', 'OperatorSalary','OperatorGameType', 'SalaryCap', 'OperatorPlayerName', 'OperatorPosition', 'OperatorRosterSlots', 'pIP', 'pW', 'pQS', 'pK', 'pBB', 'pHR', 'pH', 'pHBP', 'pER', 'pBF', 'DraftKingsPoints', 'FanDuelPoints']].reset_index(drop=True)

    return projection_df

def adjust_for_park_factors_pitchers(sals_with_vegas_lines):

    park_factors = pd.read_csv('ParkFactors.csv')
    park_adjusted_sals_with_vegas_lines = sals_with_vegas_lines.merge(park_factors, how = 'left', on = ['Game.StadiumID'])
    park_adjusted_sals_with_vegas_lines['pK'] = park_adjusted_sals_with_vegas_lines['pSO/Out'] * park_adjusted_sals_with_vegas_lines['SO'] / 100
    park_adjusted_sals_with_vegas_lines['pBB'] = park_adjusted_sals_with_vegas_lines['pBB/Out'] * park_adjusted_sals_with_vegas_lines['BB'] / 100
    park_adjusted_sals_with_vegas_lines['pHR'] = park_adjusted_sals_with_vegas_lines['pHR/Out'] * park_adjusted_sals_with_vegas_lines['HR'] / 100
    park_adjusted_sals_with_vegas_lines['pH'] = park_adjusted_sals_with_vegas_lines['pH-HR/Out'] * (park_adjusted_sals_with_vegas_lines['1B'] * 0.65 + park_adjusted_sals_with_vegas_lines['2B'] * 0.2 + park_adjusted_sals_with_vegas_lines['HR'] * 0.15) / 100

    return park_adjusted_sals_with_vegas_lines