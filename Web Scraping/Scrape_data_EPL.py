from bs4 import BeautifulSoup
import requests
import time
import codecs
from pymongo import MongoClient
import json
import re

def mainPage():
    
    for k in range(10):
        url="https://footballapi.pulselive.com/football/players?pageSize=30&compSeasons=418&altIds=true&page="+str(k)+"&type=player&id=-1&compSeasonId=418"
        # basename = 'top_pl_player_'
        website = 'https://www.premierleague.com/players/'
        print("Pulling player main data from API: ")
        response = requests.get(url = url, headers = headers, params = queryParams)
        players = json.loads(response.text)
        for i in players["content"]:
            
            player = i
            player_id =int(player['id'])
            player_name = player['name']['display']
            print(player_name, "'s data")
            player_url = str(website)+str(player_id)+str('/')+player_name.replace(" ","-")+"/stats"
            #Requesting the player's profile page
            stats ={}
            print("Accessing the players profile page")
            page = requests.get(player_url, headers=headers)
            print(player_url)
            soup = BeautifulSoup(page.text, 'html.parser')
            # print(soup)
            top_stats = soup.select("div.topStatList")[0]
            print("Scrapping player stats")
            if(player['info']['position'] == 'F' or player['info']['position'] == 'M' or player['info']['position'] == 'D'):
                app = top_stats.select("span.statappearances")[0].text
                all_time_goals = top_stats.select("span.statappearances")[0].text
                all_time_wins =  top_stats.select("span.statgoals")[0].text
                all_time_loses = top_stats.select("span.statlosses")[0].text
            elif (player['info']['position'] == 'G' ):
                app = top_stats.select("span.statappearances")[0].text
                all_time_goals = top_stats.select("span.statclean_sheet")[0].text
                all_time_wins =  top_stats.select("span.statwins")[0].text
                all_time_loses = top_stats.select("span.statlosses")[0].text
            normal_stat = soup.select("ul.normalStatList")[0]
            for item in normal_stat.find_all("li"):
                stat = item.select("span.allStatContainer")
                
                for j in stat:
                    stat_field = j['data-stat']
                    #Regex to remove all the characters and spaces
                    stat_value = re.sub('[^A-Za-z0-9]+', '' ,j.text)
                    stats.update({stat_field:stat_value})
            # print(stats)
            
            stats.update({"all_time_app":re.sub('[^A-Za-z0-9]+', '' ,app), "all_time_goals": re.sub('[^A-Za-z0-9]+', '' ,all_time_goals), "all_time_wins": re.sub('[^A-Za-z0-9]+', '' ,all_time_wins), "all_time_loses" :re.sub('[^A-Za-z0-9]+', '' ,all_time_loses)})
            player.update({"stats":stats})
            print(player)
            #accessing to transfermarket
            market_value = accessToTransferMarket(player_name,player['nationalTeam']['country'])
            player.update({"current_market_value" : market_value})

            #pushing to the DB 
            pushToMongo(player)
            time.sleep(3)
            print("Combined with data")




def pushToMongo(data):
    result = db.stats.insert_one(data)
    print(result)
    
    
    

def accessToTransferMarket(player_name,country):
    headers_tm = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'} 
    tm_url = "https://www.transfermarkt.us/schnellsuche/ergebnis/schnellsuche?query="
    tm_player_url = str(tm_url)+player_name.replace(" ","+")
    print(tm_player_url)
    page = requests.get(tm_player_url, headers=headers_tm)
    soup = BeautifulSoup(page.text, 'html.parser')
    
    get_items = soup.select("table.items")
    if(get_items):
        list_item = get_items[0].find_all("tr",{"class": ["odd", "even"]})
        if(len(list_item)>1):
            print(len(list_item))
            for i in list_item:
            
                if (i.select("table.inline-table td.hauptlink a")[0].text == str(player_name) and i.select("img.flaggenrahmen")[0]['title'] == str(country)):
                    link =list_item[0].select("table.inline-table td.hauptlink a")
                    value = getMarketValue(link[0]['href'],headers_tm)
                else:
                    value = "no_value"
                    
        else:
            link =list_item[0].select("table.inline-table td.hauptlink a")[0]['href']
            value= getMarketValue(link,headers_tm)
        return value
    else:
        return "player_not_found"



def getMarketValue(link,headers_tm):
    transfer_market_page = 'https://www.transfermarkt.us' + link
    print(transfer_market_page)
    tm_personal = requests.get(transfer_market_page, headers=headers_tm)
    tm_ppage = BeautifulSoup(tm_personal.text, 'html.parser')
    market_value = tm_ppage.select("div.auflistung div.zeile-oben div.right-td")
    if(market_value):
        return market_value[0].text
    else:
        return "no_value"
if __name__ == '__main__':
    playerList = []
    SEASON_ID = 418
    headers = {
    "content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "DNT": "1",
    "Origin": "https://www.premierleague.com",
    "Referer": "https://www.premierleague.com/players",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"
    }
    queryParams = {
        "pageSize": 1500,
        "compSeasons": SEASON_ID,
        "altIds": True,
        "page": 0,
        "type": "player",
    
    }
    client = MongoClient("mongodb://localhost:27017")
    db = client.pl_players
    mainPage()
    
    # readPages()


