#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import csv
import json
import datetime
import pandas_gbq
import pydata_google_auth
from google.cloud import bigquery
#import dotenv
import google.auth
import pandas_gbq as pdgbq
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials


# In[2]:


def get_player_puuid(game_name, tag_line):
    
    # header parameters
    request_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
           "Accept-Language": "en-US,en;q=0.9",
           "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
           "Origin": "https://developer.riotgames.com",
           "X-Riot-Token": "RGAPI-e48c9295-e2d2-472e-a3ed-77381a6c6245"}
    
    # regions
    regions = ["americas", "asia", "esports", "europe"]
    
    # api key
    api_key = "RGAPI-e48c9295-e2d2-472e-a3ed-77381a6c6245"
    
        
    # check in all regions until name is found
    for reg in regions:
        # request url
        request_url = "https://"+reg+".api.riotgames.com/riot/account/v1/accounts/by-riot-id/"+game_name+"/"+tag_line+"?api_key="+api_key
        "https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/NRG%20s0m/NRG"
        # response
        response = requests.get(request_url, headers=request_headers)
        response_json = response.json()
        
        # store in result
        player_puuid = response_json['puuid']
                
        # check status code
        if response.status_code == 200:
            break
    
    return(player_puuid)


# In[3]:


def get_player_names(sheet_name, define_scope, keys_disd):
    
    # define the scope for accessing google sheets
    scope = define_scope
    
    # add credentials to the account
    creds = ServiceAccountCredentials.from_json_keyfile_dict(keys_disd, scope)
    
    # authorize the clientsheet 
    client = gspread.authorize(creds)
    
    # get the instance of the Spreadsheet
    sheet = client.open(sheet_name)
    
    # get the first sheet of the Spreadsheet
    sheet_instance = sheet.get_worksheet(0)
    
    # get all the records of the data
    records_data = sheet_instance.get_all_records()
    
    # convert the json to dataframe
    players_name = pd.DataFrame.from_dict(records_data)
    
    return(players_name)


# In[9]:


# In[4]:


def get_match_ids(puuid):
    # header params
    request_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
           "Accept-Language": "en-US,en;q=0.9",
           "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
           "Origin": "https://developer.riotgames.com",
           "X-Riot-Token": "RGAPI-e48c9295-e2d2-472e-a3ed-77381a6c6245"}
    
    #initialize data frame
    all_matches = pd.DataFrame(columns=['matchId', 'region'])
    
    # regions
    regions = ['ap','br', 'esports','eu', 'kr', 'latam','na']
    
    # type of matches
    matches_type = ['competitive']   #unrated, deathmatch
    
    #test
    i = 0
        
    # api key
    api_key = 'RGAPI-e48c9295-e2d2-472e-a3ed-77381a6c6245'
    
    for reg in regions:
        # request url
        try:
            request_url = "https://"+reg+".api.riotgames.com/val/match/v1/matchlists/by-puuid/"+puuid+"?api_key="+api_key
            # request
            puuid_matches = requests.get(request_url, headers=request_headers).json()
            df_matches = pd.json_normalize(puuid_matches, record_path = ['history'])
            #df_matches['queueId'].unique()
            df_matches_comp = df_matches[df_matches['queueId'] == 'competitive']
            df_matches_comp = df_matches_comp.drop(['gameStartTimeMillis', 'queueId'], axis = 1)
            
            # matches 
            match_list = list(set(df_matches_comp['matchId']))
            matches_1 = list(all_matches['matchId'])
            
            # filter existing matches
            matches_2 = [match_id for match_id in match_list if match_id not in matches_1]
            
            # add to data frame
            df_matches_add = pd.DataFrame(columns = ['matchId', 'region'])
            df_matches_add['matchId'] = matches_2
            df_matches_add['region'] = reg
            all_matches = all_matches.append(df_matches_add, ignore_index=True)
            
        except:
            i = 1000
            
    return(all_matches)


# In[5]:


def eval_match_ids(puuidlist, match_table, credentials):
    
    # read existing match ids
    current_matches = pandas_gbq.read_gbq("select * from `pk-data-warehouse.valorant_data.riot_match_ids`",
                                          project_id='pk-data-warehouse',
                                          credentials=credentials
                                         )
    current_matches_list = list(current_matches['matchId'])
    # Get New Match IDs
    # 2 step process 
    # Step 1 - remove same match ids from new match list
    # Step 2 - remove match ids that are already present in big query
    
    df_all_matches = pd.DataFrame(columns = ['matchId', 'region'])
    
    for puuid in puuidlist:
        df_matches_1 = get_match_ids(puuid)
        df_all_matches = df_all_matches.append(df_matches_1, ignore_index = True)
    
    # remove duplicate match IDs
    df_all_matches_1 = df_all_matches.drop_duplicates(subset='matchId', keep="first").reset_index(drop=True)
    
    # add label to check if it is present in existing data
    df_all_matches_1['drop_label'] = df_all_matches_1['matchId'].apply(lambda x : 'Yes' if (x in current_matches_list) else 'No')
    
    # remove drop_label = Yes
    df_all_matches_2 = df_all_matches_1[df_all_matches_1['drop_label'] == 'No'].drop('drop_label', axis = 1)
    
    ########## Add timestamp
    current_timestamp = datetime.datetime.now()
    ct = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    df_all_matches_2['timestamp'] = ct
    
    ########## Change column types
    df_all_matches_2 = df_all_matches_2.astype({'matchId':'string', 'region':'string'})
    df_all_matches_2['timestamp'] =  pd.to_datetime(df_all_matches_2['timestamp'], format='%Y-%m-%d %H:%M:%S')
    
    return(df_all_matches_2)


# In[11]:


# In[6]:


def get_match_data(matchid, reg, credentials,log, df_maps1,df_agents1,df_weapons1):
    
    start = time.time()

    # header params
    request_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
                       "Accept-Language": "en-US,en;q=0.9",
                       "Accept-Charset": "application/x-www-form-urlencoded; charset=UTF-8",
                       "Origin": "https://developer.riotgames.com",
                       "X-Riot-Token": "RGAPI-e48c9295-e2d2-472e-a3ed-77381a6c6245"}

    # api key
    apikey = 'RGAPI-e48c9295-e2d2-472e-a3ed-77381a6c6245'

    # api request url
    request_url = "https://"+reg+".api.riotgames.com/val/match/v1/matches/" + matchid + "?api_key="+apikey

    # get response
    response_match = requests.get(request_url, headers=request_headers).json()


    ################################################################################################
    ################################## MatchInfo ###################################################
    ################################################################################################

    pd_matchinfo = pd.json_normalize(response_match['matchInfo'])

    # get match start date and time and map
    match_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(pd_matchinfo['gameStartMillis'][0]/1000))
    pd_matchinfo['match_start_time'] =  pd.to_datetime(match_time, format='%Y-%m-%d %H:%M:%S')
    match_mapid = pd_matchinfo['mapId'][0]


    # fill na values
    pd_matchinfo = pd_matchinfo.fillna('')

    # Change column types
    pd_matchinfo = pd_matchinfo.astype({'matchId':'string','mapId':'string','provisioningFlowId':'string','customGameName':'string',
                                       'queueId':'string','gameMode':'string','seasonId':'string', 'gameLengthMillis':'float',
                                       'gameStartMillis':'float',
                                       'gameVersion':'string',
                                       'isCompleted':'bool'})

    # write to bigquery
    try:
        pd_matchinfo.to_gbq('valorant_data.riot_match_info', 
                            project_id='pk-data-warehouse', 
                            table_schema=[{'name' : 'match_start_time', 'type' : 'DATETIME'}],
                            chunksize=10000, 
                            reauth=False, 
                            if_exists='append', 
                            progress_bar=True, 
                            credentials=credentials)
        log['match_info'] = [True]
    except:
        log['match_info'] = [False]

    ################################################################################################
    ################################## Players ###################################################
    ################################################################################################

    pd_players = pd.json_normalize(response_match['players'])

    # add match id and date and mapid
    pd_players['matchId'] = matchid
    pd_players['match_start_time'] =  pd.to_datetime(match_time, format='%Y-%m-%d %H:%M:%S')
    pd_players['mapUrl'] = match_mapid

    # fill na values
    pd_players = pd_players.fillna('')

    # change column names
    players_nc = [cols.replace('.','_') for cols in list(pd_players.columns)]
    pd_players.columns = players_nc
    
    # join with maps to get map name
    pd_players = pd.merge(pd_players, df_maps1, how = 'left', on = 'mapUrl')
    pd_players = pd_players.rename(columns={'displayName':'mapName'})


    pd_players = pd_players.merge(df_agents1, how = 'left', left_on = 'characterId', right_on = 'uuid').drop('uuid', axis = 1)
    pd_players = pd_players.rename(columns={'displayName':'agentName'})


    ########## Change column types
    pd_players = pd_players.astype({'puuid':'string','gameName':'string','tagLine':'string','teamId':'string','partyId':'string',
                                   'characterId':'string','playerCard':'string','playerTitle':'string','matchId':'string',
                                   'competitiveTier':'float','stats_score':'float','stats_roundsPlayed':'float','stats_kills':'float',
                                   'stats_deaths':'float','stats_assists':'float','stats_playtimeMillis':'float','stats_abilityCasts_grenadeCasts':'float',
                                   'stats_abilityCasts_ability1Casts':'float','stats_abilityCasts_ability2Casts':'float','stats_abilityCasts_ultimateCasts':'float',
                                   'mapUrl':'string', 'mapName':'string','agentName':'string'})

    pd_players = pd_players[['puuid','gameName','tagLine','teamId','partyId',
                             'characterId','playerCard','playerTitle','matchId',
                             'competitiveTier','stats_score','stats_roundsPlayed','stats_kills',
                             'stats_deaths','stats_assists','stats_playtimeMillis','stats_abilityCasts_grenadeCasts',
                             'stats_abilityCasts_ability1Casts','stats_abilityCasts_ability2Casts','stats_abilityCasts_ultimateCasts',
                             'mapUrl', 'mapName','agentName','match_start_time']]

    try:
        pd_players.to_gbq('valorant_data.riot_players', 
                          project_id='pk-data-warehouse',
                          table_schema=[{'name' : 'match_start_time', 'type' : 'DATETIME'}],
                          chunksize=10000, 
                          reauth=False,
                          if_exists='append', 
                         progress_bar=True, 
                          credentials=credentials)
        log['players'] = [True]
    except:
        log['players'] = [False]



    ################################################################################################
    ################################## Teams ###################################################
    ################################################################################################

    pd_teams = pd.json_normalize(response_match['teams'])
    pd_teams['matchId'] = matchid

    # fill na values
    pd_teams = pd_teams.fillna('')

    # change column type
    pd_teams = pd_teams.astype({'teamId':'string','matchId':'string','roundsPlayed':'float',
                                'roundsWon':'float','numPoints':'float'})

    pd_teams.columns

    pd_teams = pd_teams[['teamId', 'won', 'roundsPlayed', 'roundsWon', 'numPoints', 'matchId']]

    # write to bigquery
    try:
        pd_teams.to_gbq('valorant_data.riot_teams', 
                         project_id='pk-data-warehouse', 
                         chunksize=10000, 
                         reauth=False, 
                         if_exists='append', 
                         progress_bar=True, 
                         credentials=credentials) 
        log['teams'] = [True]
    except:
        log['teams'] = [False]


    ################################################################################################
    ########################## RopundResults, Player Stats, Kills #################################
    ################################################################################################

    round_results = pd.json_normalize(response_match['roundResults'])
    # add match id and date
    round_results['matchId'] = matchid
    round_results['match_start_time'] =  pd.to_datetime(match_time, format='%Y-%m-%d %H:%M:%S')

    # get number of players in each round
    round_results['no_of_players'] = round_results.apply(lambda x: len(x.playerStats), axis=1)

    # repeat roundNumber 
    repeat_roundNum = round_results[['roundNum', 'no_of_players']]
    repeat_roundNum = repeat_roundNum.loc[repeat_roundNum.index.repeat(repeat_roundNum.no_of_players)].reset_index(drop=True)


    ##############################################################################################################
    ############################# Player Stats #############

    # Normalize Player Stats
    player_stats = pd.concat([(pd.json_normalize(row['playerStats'])) for _, row in round_results.iterrows()]).reset_index(drop = True)

    # Add roundnum to player stats
    player_stats['roundNum'] = repeat_roundNum['roundNum']

    # add match id and date
    player_stats['matchId'] = matchid
    player_stats['match_start_time'] =  pd.to_datetime(match_time, format='%Y-%m-%d %H:%M:%S')

    # aggregate damage score
    def agg_score(damage_list, metric):
        agg_score = sum(player[str(metric)] for player in damage_list)
        return agg_score

    # add columns for damage scores
    player_stats['playerDamage.score'] = player_stats.apply(lambda x : agg_score(x.damage, 'damage'), axis = 1)
    player_stats['playerDamage.legshots'] = player_stats.apply(lambda x : agg_score(x.damage, 'legshots'), axis = 1)
    player_stats['playerDamage.bodyshots'] = player_stats.apply(lambda x : agg_score(x.damage, 'bodyshots'), axis = 1)
    player_stats['playerDamage.headshots'] = player_stats.apply(lambda x : agg_score(x.damage, 'headshots'), axis = 1)


    ##############################################################################################################
    ########################## PLayer Kills ################

    def get_kills_df(kills, round_num):
        df1_kills = pd.DataFrame()
        kills_rows = pd.json_normalize(kills)
        kills_rows['roundNum'] = round_num
        #df1_kills = df1_kills.append(kills_rows, ignore_index = True)
        df1_kills = kills_rows
        return(df1_kills)

    # get player kills data frame
    player_kills = pd.concat([(get_kills_df(row.kills, row.roundNum)) for _, row in player_stats.iterrows()]).reset_index(drop = True)

    # add match id and date
    player_kills['matchId'] = matchid
    player_kills['match_start_time'] =  pd.to_datetime(match_time, format='%Y-%m-%d %H:%M:%S')

    # killer player location
    def killer_location(killer, locations_list):
        list_all = list(filter(lambda location : location['puuid'] == killer, locations_list))
        if len(list_all) == 0:
            x,y = None,None
        else:
            locations = list_all[0]['location']
            x,y = locations['x'],locations['y']
        return(x,y)

    player_kills['killerLocation.x'] = player_kills.apply(lambda x: killer_location(x.killer, x.playerLocations)[0], axis=1)
    player_kills['killerLocation.y'] = player_kills.apply(lambda x: killer_location(x.killer, x.playerLocations)[1], axis=1)
    player_kills['mapUrl'] = pd_matchinfo['mapId'][0]

    #########################################################################################################################
    ##################################### Transformations 

    def fill_na(col):
        if 'float' in str(col.dtypes):
            col = col.fillna(0)
        else:
            col = col.fillna('')
        return col

    def replace_empty_lists(col):
        if len(col) == 0:
            col = ''
        else:
            col = col
        return col

    ################################################### Round Results #######################################
    # replace na values
    round_results = round_results.fillna('')
    # replace empty lists
    round_results['plantPlayerLocations'] = round_results.apply(lambda x : replace_empty_lists(x.plantPlayerLocations), axis = 1)
    round_results['defusePlayerLocations'] = round_results.apply(lambda x : replace_empty_lists(x.defusePlayerLocations), axis = 1)
    # drop player stats
    round_results = round_results.drop(['playerStats'], axis = 1)
    # change clumn types
    round_results = round_results.astype({'roundNum':'float','roundResult':'string','roundCeremony':'string','winningTeam':'string',
                                          'bombPlanter':'string','bombDefuser':'string','plantRoundTime':'float',
                                          'plantPlayerLocations':'string', 'plantSite':'string',
                                          'defuseRoundTime':'float','defusePlayerLocations':'string',
                                          'roundResultCode':'string','plantLocation.x':'float',
                                          'plantLocation.y':'float','defuseLocation.x':'float','defuseLocation.y':'float',
                                          'matchId':'string', 'no_of_players':'float',
                                          })
    # change column names
    round_results_nc = [cols.replace('.','_') for cols in list(round_results.columns)]
    round_results.columns = round_results_nc 

    ################################################### Player Stats #######################################
    player_stats['damage'] = player_stats.apply(lambda x : replace_empty_lists(x.damage), axis = 1)
    player_stats = player_stats.astype({'puuid':'string','damage':'string','score':'float','economy.loadoutValue':'float',
                                          'economy.weapon':'string','economy.armor':'string','economy.remaining':'float',
                                          'economy.spent':'float', 'ability.grenadeEffects':'float',
                                          'ability.ability1Effects':'float','ability.ability2Effects':'float',
                                          'ability.ultimateEffects':'float','roundNum':'float',
                                          'matchId':'string','playerDamage.score':'float','playerDamage.legshots':'float',
                                          'playerDamage.bodyshots':'float', 'playerDamage.headshots':'float',
                                          })

    for i in range(len(list(player_stats.columns))):
        player_stats[list(player_stats.columns)[i]] = fill_na(player_stats[list(player_stats.columns)[i]])
    # drop player kills
    player_stats = player_stats.drop(['kills'], axis = 1)
    # change column names
    player_stats_nc = [cols.replace('.','_') for cols in list(player_stats.columns)]
    player_stats.columns = player_stats_nc

    ################################################### Player kills #######################################
    player_kills['assistants'] = player_kills.apply(lambda x : replace_empty_lists(x.assistants), axis = 1)
    player_kills['playerLocations'] = player_kills.apply(lambda x : replace_empty_lists(x.playerLocations), axis = 1)

    # flag first kill, death and trade kills
    player_kills['eventRank'] = player_kills.groupby('roundNum')['timeSinceRoundStartMillis'].rank(ascending = True)
    player_kills = player_kills.sort_values(by=['roundNum','timeSinceRoundStartMillis'], ignore_index=True)
    player_kills[['killerLagged', 'eventRankLagged']] = player_kills.groupby('roundNum')[['killer', 'eventRank']].shift(1,fill_value = '')

    player_kills['tradeKill'] = player_kills.apply(lambda x : 1 if (x['victim'] == x['killerLagged'] and x['eventRank'] == x['eventRankLagged'] + 1) else 0, axis = 1)
    player_kills['firstKillDeath'] = player_kills.apply(lambda x: 1 if (x['eventRank'] == 1) else 0, axis = 1)
    player_kills = player_kills.drop(['killerLagged', 'eventRankLagged'],axis=1)

    player_kills = player_kills.astype({'roundNum':'float','timeSinceGameStartMillis':'float','timeSinceRoundStartMillis':'float',
                                        'killer':'string',
                                          'victim':'string','assistants':'string','playerLocations':'string',
                                          'victimLocation.x':'float', 'victimLocation.y':'float',
                                          'finishingDamage.damageType':'string','finishingDamage.damageItem':'string',
                                          'finishingDamage.isSecondaryFireMode':'string','killerLocation.x':'float',
                                          'killerLocation.y':'float', 'matchId':'string','mapUrl':'string',
                                        'eventRank':'float', 'firstKillDeath':'float','tradeKill':'float'
                                          })

    for i in range(len(list(player_kills.columns))):
        player_kills[list(player_kills.columns)[i]] = fill_na(player_kills[list(player_kills.columns)[i]])
    # change column names
    player_kills_nc = [cols.replace('.','_') for cols in list(player_kills.columns)]
    player_kills.columns = player_kills_nc


    round_results = round_results[['roundNum', 'roundResult', 'roundCeremony', 'winningTeam',
           'bombPlanter', 'bombDefuser', 'plantRoundTime', 'plantPlayerLocations',
           'plantSite', 'defuseRoundTime', 'defusePlayerLocations',
           'roundResultCode', 'plantLocation_x', 'plantLocation_y',
           'defuseLocation_x', 'defuseLocation_y', 'matchId', 'match_start_time',
           'no_of_players']]
    # write all to bigquery
    try:
        round_results.to_gbq('valorant_data.riot_round_results', 
                      project_id='pk-data-warehouse',
                      table_schema=[{'name' : 'match_start_time', 'type' : 'DATETIME'}],
                      chunksize=10000, 
                      reauth=False, 
                      if_exists='append', 
                      progress_bar=True, 
                      credentials=credentials)
        log['round_results'] = [True]
    except:
        log['round_results'] = [False]



    # player_stats
    player_stats = player_stats[['puuid', 'damage', 'score', 'economy_loadoutValue', 'economy_weapon',
           'economy_armor', 'economy_remaining', 'economy_spent',
           'ability_grenadeEffects', 'ability_ability1Effects',
           'ability_ability2Effects', 'ability_ultimateEffects', 'roundNum',
           'matchId', 'match_start_time', 'playerDamage_score',
           'playerDamage_legshots', 'playerDamage_bodyshots',
           'playerDamage_headshots']]
    try:
        player_stats.to_gbq('valorant_data.riot_player_stats', 
                      project_id='pk-data-warehouse',
                      table_schema=[{'name' : 'match_start_time', 'type' : 'DATETIME'}],
                      chunksize=10000, 
                      reauth=False, 
                      if_exists='append', 
                      progress_bar=True, 
                      credentials=credentials)
        log['player_stats'] = [True]
    except:
        log['player_stats'] = [False]
    #
    player_kills = player_kills[['timeSinceGameStartMillis', 'timeSinceRoundStartMillis', 'killer',
           'victim', 'assistants', 'playerLocations', 'victimLocation_x',
           'victimLocation_y', 'finishingDamage_damageType',
           'finishingDamage_damageItem', 'finishingDamage_isSecondaryFireMode',
           'roundNum', 'matchId', 'match_start_time', 'killerLocation_x',
           'killerLocation_y', 'mapUrl', 'eventRank', 'tradeKill',
           'firstKillDeath']]
    try:
        player_kills.to_gbq('valorant_data.riot_player_kills', 
                      project_id='pk-data-warehouse',
                      table_schema=[{'name' : 'match_start_time', 'type' : 'DATETIME'}],
                      chunksize=10000, 
                      reauth=False, 
                      if_exists='append', 
                      progress_bar=True, 
                     credentials=credentials)
        log['player_kills'] = [True]
    except:
        log['player_kills'] = [False]

    ###################################################################################################
    #################################### consolidated table ###########################################
    ###################################################################################################

    df_base = player_stats.drop(['damage','economy_armor','ability_grenadeEffects', 'ability_ability1Effects',
                                'ability_ability2Effects', 'ability_ultimateEffects', 'match_start_time'], axis = 1)

    # get round level stats
    df_rounds = round_results[['matchId', 'roundNum', 'roundResult','roundCeremony','winningTeam']]
    df_rounds['ceremony'] = df_rounds['roundCeremony'].apply(lambda x : x[8:])
    df_rounds = df_rounds.drop('roundCeremony', axis = 1)

    # trade kills and first kills / deaths
    df_trade_kills = player_kills.groupby(['roundNum','killer']).agg({'tradeKill':'sum', 'firstKillDeath': 'sum', 'victim': 'count'}).reset_index()
    df_trade_kills = df_trade_kills.rename(columns={'firstKillDeath':'firstKill','victim':'totalKills'})
    df_first_deaths = player_kills.groupby(['roundNum','victim'])['firstKillDeath'].agg('sum').reset_index()
    df_first_deaths = df_first_deaths.rename(columns={'firstKillDeath':'firstDeath'})

    # player ace
    df_player_ace = player_kills.groupby(['roundNum','killer'])['victim'].nunique().reset_index()
    df_player_ace['playerAce'] = df_player_ace['victim'].apply(lambda x : 1 if x == 5 else 0)
    df_player_ace = df_player_ace.drop('victim', axis = 1)

    # last kill
    df_last_kill = player_kills.groupby('roundNum').agg(lambda x: x.iloc[-1]).reset_index()
    df_last_kill1 = df_last_kill[['roundNum', 'killer']]
    df_last_kill1['lastKill'] = 1

    # bomb planter
    df_bomb_plant = round_results[round_results['bombPlanter'] != ''][['roundNum','bombPlanter','plantRoundTime','plantSite']]
    df_bomb_plant['bombPlantFlag'] = 1
    df_def_plant = round_results[round_results['bombDefuser'] != ''][['roundNum','bombDefuser','defuseRoundTime']]
    df_def_plant['bombDefuseFlag'] = 1

    # join all tables
    # get weapons name and category
    df_base.loc[:,'economy_weapon'] = df_base.loc[:,'economy_weapon'].apply(lambda x: x.lower())
    df_base = df_base.merge(df_weapons1, how = 'left', left_on = 'economy_weapon', right_on = 'uuid').drop('uuid', axis = 1)
    # round results
    df_base = df_base.merge(df_rounds, how = 'left', on = ['matchId', 'roundNum'])
    # kills
    df_base = df_base.merge(df_trade_kills, how = 'left', left_on = ['roundNum','puuid'], right_on = ['roundNum', 'killer']).drop('killer', axis = 1)
    df_base = df_base.merge(df_first_deaths, how = 'left', left_on = ['roundNum','puuid'], right_on = ['roundNum', 'victim']).drop('victim', axis = 1)
    df_base = df_base.merge(df_last_kill1, how = 'left', left_on = ['roundNum','puuid'], right_on = ['roundNum', 'killer']).drop('killer', axis = 1)
    # bomb plants and defuse
    df_base = df_base.merge(df_bomb_plant, how = 'left', left_on = ['roundNum','puuid'], right_on = ['roundNum', 'bombPlanter']).drop('bombPlanter', axis = 1)
    df_base = df_base.merge(df_def_plant, how = 'left', left_on = ['roundNum','puuid'], right_on = ['roundNum', 'bombDefuser']).drop('bombDefuser', axis = 1)
    # player ace
    df_base = df_base.merge(df_player_ace, how = 'left', left_on = ['roundNum','puuid'], right_on = ['roundNum', 'killer']).drop('killer', axis = 1)

    # impute NaN values
    for i in range(len(list(df_base.columns))):
            df_base[list(df_base.columns)[i]] = fill_na(df_base[list(df_base.columns)[i]])

    df_base = df_base[['puuid', 'score', 'economy_loadoutValue', 'economy_weapon',
           'economy_remaining', 'economy_spent', 'roundNum', 'matchId',
           'playerDamage_score', 'playerDamage_legshots', 'playerDamage_bodyshots',
           'playerDamage_headshots', 'weaponName', 'weaponCategory', 'roundResult',
           'winningTeam', 'ceremony', 'tradeKill', 'firstKill', 'totalKills',
           'firstDeath', 'lastKill', 'plantRoundTime', 'plantSite',
           'bombPlantFlag', 'defuseRoundTime', 'bombDefuseFlag', 'playerAce']]

    # write to bigquery
    try:
        df_base.to_gbq('valorant_data.player_round_data', 
                      project_id='pk-data-warehouse',
                      table_schema=[{'name' : 'match_start_time', 'type' : 'DATETIME'}],
                      chunksize=10000, 
                      reauth=False, 
                     if_exists='append', 
                     progress_bar=True, 
                      credentials=credentials)
        log['player_round_level'] = [True]
    except:
        log['player_round_level'] = [False]

    end = time.time()
    #print("time taken - " + str(end - start) + " seconds")
    log['time_taken'] = round(end - start,2)
    
    return(log)


# ### Credentials

# In[7]:


def create_keyfile_dict():
    variables_keys = {
        "type": ("service_account"),
        "project_id": ("pk-data-warehouse"),
        "private_key_id": ("664b9a40292a0c562a8adb12237c92ef5796db81"),
        "private_key": ("-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDZuApgS734eHF1\nHbXMrHrsO3jvLWDL0rHP+j4N5bIJOTBMR6EM3xbgTorxn30bZYoB2jBKH4MVS1J9\nu7OnTEBS+BeT8OPLEVIAVwZJ4/OyOBQM9zc0kX12C7p5oaRu1gh3m1U2AK3p3AL4\nQowR6L+sYRxEeJbx0rRTNHPuAXVrCOv59h9HW9fxmrB8xxLRCamc7kHE3yPt4ZOJ\nhfuiDDcrnDeWa7H6m9svilnDEYamLaIOZJe0IF7dIPfL/x32PXZ3d3U3dRTgbhWt\nO6LdIZ3V475ZvRi4XK5/VkdAE2wIbQMjjcetbQPiXLibW3188k+1dULCGC/hihSm\n7PUvZxWdAgMBAAECggEACJgeOLzkq9k9SU057vFkDM0wMejNqF4RzM7pAi1uALU0\nNd6h+dsYkRQooe8Uxi8U6ovNWi+yQfXuNK5jIhh9dYj7jUh2BX8SiJbm1aXc6c0S\n5Ywgrr+Lf0xpOQHrdsCWJvqHu5D9THz11QzzcIWKg2h18DH+a19Q5PPuG5gNJYNq\n9gsIMUqq/2od+qkc/WwinynujCimA8PyvMWTa26WTjSBsWOKUN04fnwJ3NcIHORu\nH7Vgxe5rg2t0ZsNhzcnqi8OixXX+lSShz2dAs+BYX/DI9wGSzxSGQI7xct3nCvsT\nIhpWqTOO+9QXNcqrCI2sSJ/Ccpi5BTyFeuTzX4hkIQKBgQD9NB+XlMl8VgqO9YWe\nNn05gBjn1OFc01s+9ZzA+Oa8OaiwmEKmerJfTFL3oKsdQeHkcoMpUraROUHToBUj\nZoISWaNA+sx48FS21vrJK9M2f71U8vfxf6+bLGC/1/XmVlWDUJ3PpTWJnQspolGc\n68qwVKVFhocNyTIlmYHRY6rsWQKBgQDcH5eSDlFp8rtvVHthJ78yuukr0JpPOf8b\nNfhzEKdvlfDEQdLIw1jfkNT8hKyTjgBu7Mb2UN5UbzrkOWS6ORLvLojYZVpUk6cU\nnb43sDaHxBMf5cbakIQsc2W+pKRPR/+9D3Oz/uSiA06vV9nNP6TRhQa0duyB/OEE\nMBwMGPG65QKBgHVvg2eOfpTIY0VC4qIqq5HLs8FynsOq05sz12w5BOsv4ulk9SS5\nq5k/kQuA3VxIfjiyU9sKndplL/6zNJqipJlWMb2llMhSzzeuJcrAJMoWHP4VYqID\nkrLptkXEKCyYxjiNX2Q7P8V/rPm2axvY0L1PxQemCWV/d16w/+DZ1SChAoGBAJdg\nTm7N0+UlXQqxsWtd1xm++gz8lrG3M5113xbaNBsDpSCCF7+iv2J3ilDhuIB8ngWq\nZSUM9ehg2cILSh7akw/TE56lDJqvM6500FQhpU52Y6SC8t5plcuvzB9vv+MZo2BT\n2QDGYFqeJaVlp82DYAgKEFNYmUrHKcMkhpU3Lj2VAoGBALvyXLo/FlexQn0YFNgQ\nhfWSfBRnz5y5/B806u0Uk2JRRT80mzGo0THm7p0+Qv+DMuIofOexGP6Ib+M02z1w\nOwbPjxHvYWoNBGkuVMhmzPF2FtOZQ879uK2XUcGvu6C5VWAkYfnNVi6+/wSbqcqJ\nyZruPuUwvrzqK9LH7HgXxhk0\n-----END PRIVATE KEY-----\n"),
        "client_email": ("my-service-account@pk-data-warehouse.iam.gserviceaccount.com"),
        "client_id": ("102682127692952047329"),
        "auth_uri": ("https://accounts.google.com/o/oauth2/auth"),
        "token_uri": ("https://oauth2.googleapis.com/token"),
        "auth_provider_x509_cert_url": ("https://www.googleapis.com/oauth2/v1/certs"),
        "client_x509_cert_url": ("https://www.googleapis.com/robot/v1/metadata/x509/my-service-account%40pk-data-warehouse.iam.gserviceaccount.com")
    }
    return variables_keys


# ### Entry Point Function

# In[8]:

def hello_pubsub():
    
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_info({
        "type": ("service_account"),
        "project_id": ("pk-data-warehouse"),
        "private_key_id": ("664b9a40292a0c562a8adb12237c92ef5796db81"),
        "private_key": ("-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDZuApgS734eHF1\nHbXMrHrsO3jvLWDL0rHP+j4N5bIJOTBMR6EM3xbgTorxn30bZYoB2jBKH4MVS1J9\nu7OnTEBS+BeT8OPLEVIAVwZJ4/OyOBQM9zc0kX12C7p5oaRu1gh3m1U2AK3p3AL4\nQowR6L+sYRxEeJbx0rRTNHPuAXVrCOv59h9HW9fxmrB8xxLRCamc7kHE3yPt4ZOJ\nhfuiDDcrnDeWa7H6m9svilnDEYamLaIOZJe0IF7dIPfL/x32PXZ3d3U3dRTgbhWt\nO6LdIZ3V475ZvRi4XK5/VkdAE2wIbQMjjcetbQPiXLibW3188k+1dULCGC/hihSm\n7PUvZxWdAgMBAAECggEACJgeOLzkq9k9SU057vFkDM0wMejNqF4RzM7pAi1uALU0\nNd6h+dsYkRQooe8Uxi8U6ovNWi+yQfXuNK5jIhh9dYj7jUh2BX8SiJbm1aXc6c0S\n5Ywgrr+Lf0xpOQHrdsCWJvqHu5D9THz11QzzcIWKg2h18DH+a19Q5PPuG5gNJYNq\n9gsIMUqq/2od+qkc/WwinynujCimA8PyvMWTa26WTjSBsWOKUN04fnwJ3NcIHORu\nH7Vgxe5rg2t0ZsNhzcnqi8OixXX+lSShz2dAs+BYX/DI9wGSzxSGQI7xct3nCvsT\nIhpWqTOO+9QXNcqrCI2sSJ/Ccpi5BTyFeuTzX4hkIQKBgQD9NB+XlMl8VgqO9YWe\nNn05gBjn1OFc01s+9ZzA+Oa8OaiwmEKmerJfTFL3oKsdQeHkcoMpUraROUHToBUj\nZoISWaNA+sx48FS21vrJK9M2f71U8vfxf6+bLGC/1/XmVlWDUJ3PpTWJnQspolGc\n68qwVKVFhocNyTIlmYHRY6rsWQKBgQDcH5eSDlFp8rtvVHthJ78yuukr0JpPOf8b\nNfhzEKdvlfDEQdLIw1jfkNT8hKyTjgBu7Mb2UN5UbzrkOWS6ORLvLojYZVpUk6cU\nnb43sDaHxBMf5cbakIQsc2W+pKRPR/+9D3Oz/uSiA06vV9nNP6TRhQa0duyB/OEE\nMBwMGPG65QKBgHVvg2eOfpTIY0VC4qIqq5HLs8FynsOq05sz12w5BOsv4ulk9SS5\nq5k/kQuA3VxIfjiyU9sKndplL/6zNJqipJlWMb2llMhSzzeuJcrAJMoWHP4VYqID\nkrLptkXEKCyYxjiNX2Q7P8V/rPm2axvY0L1PxQemCWV/d16w/+DZ1SChAoGBAJdg\nTm7N0+UlXQqxsWtd1xm++gz8lrG3M5113xbaNBsDpSCCF7+iv2J3ilDhuIB8ngWq\nZSUM9ehg2cILSh7akw/TE56lDJqvM6500FQhpU52Y6SC8t5plcuvzB9vv+MZo2BT\n2QDGYFqeJaVlp82DYAgKEFNYmUrHKcMkhpU3Lj2VAoGBALvyXLo/FlexQn0YFNgQ\nhfWSfBRnz5y5/B806u0Uk2JRRT80mzGo0THm7p0+Qv+DMuIofOexGP6Ib+M02z1w\nOwbPjxHvYWoNBGkuVMhmzPF2FtOZQ879uK2XUcGvu6C5VWAkYfnNVi6+/wSbqcqJ\nyZruPuUwvrzqK9LH7HgXxhk0\n-----END PRIVATE KEY-----\n"),
        "client_email": ("my-service-account@pk-data-warehouse.iam.gserviceaccount.com"),
        "client_id": ("102682127692952047329"),
        "auth_uri": ("https://accounts.google.com/o/oauth2/auth"),
        "token_uri": ("https://oauth2.googleapis.com/token"),
        "auth_provider_x509_cert_url": ("https://www.googleapis.com/oauth2/v1/certs"),
        "client_x509_cert_url": ("https://www.googleapis.com/robot/v1/metadata/x509/my-service-account%40pk-data-warehouse.iam.gserviceaccount.com")
    },)

    sheet = 'Players List'


    ### Google cloud authentication
    SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

    sheet_scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    keys_location = create_keyfile_dict()

    players_name = get_player_names(sheet, sheet_scope, keys_location)

    # initialize player puuid
    puuid_list = []


    # In[9]:


    # get puuid for each player name
    for i in range(len(players_name)):
        game_name = players_name.loc[i,'gameName']
        tag_line = players_name.loc[i,'tagLine']

        try:
            puuid = get_player_puuid(game_name, tag_line)
            puuid_list.append(puuid)
        except:
            print(game_name,"not found")


    print('starts pushing into Bigquery')

    df_matches = eval_match_ids(puuid_list, 'riot_match_ids', credentials)


    # In[ ]:


    df_matches.to_gbq('valorant_data.riot_match_ids', 
                project_id='pk-data-warehouse', 
                chunksize=10000, 
                reauth=False, 
                if_exists='append', 
                table_schema=[{'name':'matchId','type':'STRING'},{'name':'timestamp','type':'DATETIME'}], 
                progress_bar=True, 
                credentials=credentials)


    # In[10]:


    # read all master tables
    # used in get match data
    df_maps = pandas_gbq.read_gbq(
        "select * from `pk-data-warehouse.valorant_data.riot_all_maps`",
        project_id='pk-data-warehouse',
        credentials=credentials
    )

    df_agents = pandas_gbq.read_gbq(
        "select * from `pk-data-warehouse.valorant_data.riot_all_agents`",
        project_id='pk-data-warehouse',
        credentials=credentials
    )

    df_weapons = pandas_gbq.read_gbq(
        "select * from `pk-data-warehouse.valorant_data.riot_all_weapons`",
        project_id='pk-data-warehouse',
        credentials=credentials
    )

    df_maps1 = df_maps[['mapUrl', 'displayName']]
    df_maps1.drop_duplicates(keep='first', inplace=True, ignore_index=True)

    df_agents1 = df_agents[['uuid', 'displayName']]
    df_agents1.drop_duplicates(keep='first', inplace=True, ignore_index=True)

    df_weapons1 = df_weapons[['uuid', 'displayName', 'category']]
    df_weapons1['weaponCategory'] = df_weapons1['category'].apply(lambda x : x.split('::')[1])
    df_weapons1 = df_weapons1.drop('category', axis = 1)
    df_weapons1 = df_weapons1.rename(columns={'displayName':'weaponName'})
    df_weapons1.drop_duplicates(keep='first', inplace=True, ignore_index=True)


    # In[14]:


    # upload data for all matches to bigquery
    # upload data for all matches to bigquery
    success_list = []
    error_list = []
    if len(df_matches)>0:
        for i in range(len(df_matches)):
            match_id = df_matches.iloc[i,0]
            reg_name = df_matches.iloc[i,1]

            # initialize dictionary
            log = dict()
            log['matchId'] = match_id
            log['match_info'] = [False]
            log['players'] = [False]
            log['teams'] = [False]
            log['round_results'] = [False]
            log['player_stats'] = [False]
            log['player_kills'] = [False]
            log['player_round_level'] = [False]
            log['time_taken'] = [0.0]

            try:
                log = get_match_data(match_id, reg_name, credentials, log,df_maps1,df_agents1,df_weapons1)
                success_list.append(log)
            except:
                error_list.append(match_id)

            df_logs = pd.DataFrame.from_dict(log,orient='columns')
            df_logs = df_logs.astype({'matchId':'string','time_taken':'float'})
            df_logs.to_gbq('valorant_data.etl_logs', 
                        project_id='pk-data-warehouse',
                        chunksize=10000, 
                        reauth=False, 
                        if_exists='append', 
                        progress_bar=False, 
                        credentials=credentials)
    
    print('end of the func')
    print('success list',success_list)
    print('error list', error_list)
    return df_logs


if __name__ == '__main__':
    hello_pubsub();

