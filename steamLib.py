import requests
import pandas as pd

# Get the history of a single game from steamcharts.com
# Simulate a browser request to get the data
# Add a cookie to the request to avoid getting a 403 error
# The request url should be like this https://steamcharts.com/app/gameid/chart-data.json where gameid is the id of the game
def get_steamcharts_history(gameid):
    #Check if there is a csv file with the gameid name
    #If there is, load it and return it
    #If there isn't, make the request, save the data in a csv file and return the data

    filename = f'steamcharts/{gameid}.csv'
    try:
        df = pd.read_csv(filename)
        return df
    except:
        pass

    data = get_steamcharts_history_raw(gameid)
    if len(data) == 0:
        #return an empty dataframe
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=['timestamp', gameid])
    df = df.set_index('timestamp')
    df.to_csv(filename, index=True)
    return df

def get_item_price_history(itemName,gameid):
    #Create an hash of the itemName
    #Check if there is a csv file with the hash as name
    #If there is, load it and return it
    #If there isn't, make the request, save the data in a csv file and return the data

    filename = f'item_price_history/{hash(itemName)}.csv'
    try:
        df = pd.read_csv(filename)
        return df
    except:
        pass

    data = get_item_price_history_raw(itemName,gameid)
    if len(data) == 0:
        #return an empty dataframe
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=['timestamp', 'price', 'volume'])
    df = df.set_index('timestamp')
    df.to_csv(filename, index=True)
    return df

def get_item_price_history_raw(itemName,gameid):
    #Make a request to the url https://steamcommunity.com/market/listings/[gameid]/[itemName]
    #Parse the html and find the text "var line1="
    #The text between "var line1=" and ";" is a json with the price history of the item as a list of lists composed of [timestamp, price, volume]

    url = f'https://steamcommunity.com/market/listings/{gameid}/{itemName}'
    print(url)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0',
        'Accept': '*/*',
        'Accept-Language': 'it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3',
        #'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
        #'Connection': 'keep-alive',
        'Referer': f'https://steamcommunity.com/market/listings/{gameid}/{itemName}',
        'Cookie': 'dnsDisplayed=undefined; ccpaApplies=false; signedLspa=undefined; _sp_su=false; consentUUID=a3cebe9b-a750-4c4b-954f-6d8b70486a82_18; ccpaUUID=28a04786-458e-444d-bd1b-ad50e8352e05',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(response.status_code)
        return []
    html = response.text
    start = html.find('var line1=') + len('var line1=')
    end = html.find(';', start)
    json = html[start:end]
    return json

def get_steamcharts_history_raw(gameid):

    url = f'https://steamcharts.com/app/{gameid}/chart-data.json'
    print(url)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3',
        #'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
        #'Connection': 'keep-alive',
        'Referer': f'https://steamcharts.com/app/{gameid}',
        'Cookie': 'dnsDisplayed=undefined; ccpaApplies=false; signedLspa=undefined; _sp_su=false; consentUUID=a3cebe9b-a750-4c4b-954f-6d8b70486a82_18; ccpaUUID=28a04786-458e-444d-bd1b-ad50e8352e05',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'TE': 'trailers',
    }
    response = requests.get(url, headers=headers)
    # The response is a json with the following structure
    # {
    #   "data": [
    #     [timestamp, players],
    #     [timestamp, players],
    #     ...
    #   ]
    # }
    # The data is a list of lists
    # The first element of each list is the timestamp
    # The second element of each list is the number of players
    if response.status_code != 200:
        print(response.status_code)
        return []
    return response.json()

def get_steam_app_list():
# Get all the games names and corresponding ids from the steam api
#http://api.steampowered.com/ISteamApps/GetAppList/v0002/
    url = 'http://api.steampowered.com/ISteamApps/GetAppList/v0002/'
    response = requests.get(url)
    if response.status_code != 200:
        print(response.status_code)
        print(response.text)
        return []
    return response.json()

def get_all_items_for_game(gameid):
    #Query the page https://steamcommunity.com/market/search/render/?&appid=[gameid]&norender=1#
    #Get the total_count from the json
    #Iterate over the pages where the page number is the index of the iteration multiplied by 100
    #Until the number of items in the page is less than 100
    #The query url is https://steamcommunity.com/market/search/render/?query=&start=[page*100]&count=100&search_descriptions=0&sort_column=price&sort_dir=desc&appid=[gameid]&norender=1#
    #Save the data from the results node of the json in a dictionary with the following structure {itemName: {name: name, price: sell_price/100, listings: sell_listings}}
    #Return the dictionary

    url = f'https://steamcommunity.com/market/search/render/?&appid={gameid}&norender=1#'
    print(url)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3',
        #'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
        #'Connection': 'keep-alive',
        'Referer': f'https://steamcommunity.com/market/search?appid={gameid}',
        'Cookie': 'dnsDisplayed=undefined; ccpaApplies=false; signedLspa=undefined; _sp_su=false; consentUUID=a3cebe9b-a750-4c4b-954f-6d8b70486a82_18; ccpaUUID=28a04786-458e-444d-bd1b-ad50e8352e05',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(response.status_code)
        return []
    json = response.json()
    total_count = json['total_count']
    results = json['results']
    items = {}
    for result in results:
        itemName = result['hash_name']
        name = result['name']
        sell_price = result['sell_price']
        sell_listings = result['sell_listings']
        items[itemName] = {'name': name, 'price': sell_price/100, 'listings': sell_listings}
    for page in range(1, total_count//100 + 1):
        url = f'https://steamcommunity.com/market/search/render/?query=&start={page*100}&count=100&search_descriptions=0&sort_column=price&sort_dir=desc&appid={gameid}&norender=1#'
        print(url)
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(response.status_code)
            return []
        json = response.json()
        results = json['results']
        for result in results:
            itemName = result['hash_name']
            name = result['name']
            sell_price = result['sell_price']
            sell_listings = result['sell_listings']
            items[itemName] = {'name': name, 'price': sell_price/100, 'listings': sell_listings}
        if len(results) < 100:
            break
    return items
