import pandas as pd
import requests
import json
import datetime
import logging


#Creating logger
logging.basicConfig(
    filename='C:\\Users\\dilovar.mashrabov\\Desktop\\Junior Semester_2\\Data Science\\Project\\logs\\app.log', 
    filemode='a', 
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
    datefmt='%d-%b-%y %H:%M:%S')

#Function to get id and start time of the games
def get_id(id = ''):
    """This function is used to gather game_id's"""
    url = "https://community-dota-2.p.rapidapi.com/IDOTA2Match_570/GetMatchHistory/V001/"
    if id == '':
        querystring = { 
            "key": "8279A03D656F8348F2E483FBCDDB37EB",
            "format": "JSON", 
            }
    else:
        querystring = { 
            "key": "8279A03D656F8348F2E483FBCDDB37EB",
            "format": "JSON", 
            "start_at_match_id": str(id)}
    
    headers = {
        'x-rapidapi-host': "community-dota-2.p.rapidapi.com",
        'x-rapidapi-key': "cb9f589285mshecd88fb28c51597p1a4ee7jsn571b68d1f215"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)
    return response.text

# Function to get match information
def get_match_data(id):
    """This function is used to get match data from match_id"""
    url = "https://community-dota-2.p.rapidapi.com/IDOTA2Match_570/GetMatchDetails/V001/"

    querystring = {"key":"8279A03D656F8348F2E483FBCDDB37EB","match_id":str(id),"matches_requested":"20"}

    headers = {
        'x-rapidapi-host': "community-dota-2.p.rapidapi.com",
        'x-rapidapi-key': "cb9f589285mshecd88fb28c51597p1a4ee7jsn571b68d1f215"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    json_response = json.loads(response.text)
    return json_response

# Function used to parse through json that was taken from api
def exctract_match_data(response):
    """This function parses through json that was taken from get_match_data and return all needed info about match"""
    response = response['result']

    result = [response['match_id'], response['radiant_win'], response['cluster'], response['game_mode']]
    for player in response['players']:
        result.append(player['account_id']) 
        result.append(player['player_slot'])
        result.append(player['hero_id'])
        
    return result 

#Preparing lists  
all_id = []
all_mode = []
match_id = []
start_time = []

#Opening all necessary files
id_data = pd.read_csv("C:\\Users\\dilovar.mashrabov\\Desktop\\Junior Semester_2\\Data Science\\Project\\data\\match_id.csv").drop('Unnamed: 0', axis=1)
match_data = pd.read_csv("C:\\Users\\dilovar.mashrabov\\Desktop\\Junior Semester_2\\Data Science\\Project\\data\\match_data.csv").drop('Unnamed: 0', axis=1)
all_matches = pd.read_csv("C:\\Users\\dilovar.mashrabov\\Desktop\\Junior Semester_2\\Data Science\\Project\\data\\all_matches.csv").drop('Unnamed: 0', axis=1)
id_data.set_index('match_id')
match_data.set_index('match_id')
all_matches.set_index('match_id')
start_time = datetime.datetime.now()#setting  execution start time


#Setting the main loop that will gather data for long time
for g in range(100):
    json_response = json.loads(get_id())
    used = 0
    for j in range(5):
        try:
            for i in json_response['result']['matches']:
                game = get_match_data(i['match_id'])
                all_id.append(i['match_id'])
                all_mode.append(game['result']['game_mode'])
                
                #saving id and game mode of all games
                to_append_all = pd.Series([i['match_id'], game['result']['game_mode']], index = ['match_id', 'game_mode'])
                all_matches = all_matches.append(to_append_all, ignore_index=True)

                if game['result']['game_mode'] == 22: #22 stands for Ranked all pick
                    used +=1 #Counter for game mode 22
                    #Appending match_data and id_dataset dataframes
                    to_append_match = pd.Series(exctract_match_data(game), index=match_data.columns)
                    to_append_id = pd.Series([i['match_id'], i['start_time']], index=['match_id', 'start_time'])
                    id_data = id_data.append(to_append_id, ignore_index=True)
                    match_data = match_data.append(to_append_match, ignore_index=True)
        except:
            logging.error(game['result'], exc_info=True)

        #changing match id for new request
        json_response = json.loads(get_id(all_id[-1]))
    
    # Saving all results that were collected oon this iteration
    all_matches.to_csv('C:\\Users\\dilovar.mashrabov\\Desktop\\Junior Semester_2\\Data Science\\Project\\data\\all_matches.csv')
    id_data.to_csv("C:\\Users\\dilovar.mashrabov\\Desktop\\Junior Semester_2\\Data Science\\Project\\data\\match_id.csv")
    match_data.to_csv("C:\\Users\\dilovar.mashrabov\\Desktop\\Junior Semester_2\\Data Science\\Project\\data\\match_data.csv")
    end_time = datetime.datetime.now()
    print(str(g), str(end_time-start_time), used)
