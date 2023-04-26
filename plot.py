import requests
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.dates as mdates

all_games = [1448230,304930,440,252490,218620,232090,570,322330,730,290340,866510,614910,603750,282800,253230,562430,270880,450860,530700,583950,582810,489940,706990,1723560,296300,618140,238460,650580,1142220,550650,225600,575950,709840,531960,598780,529840,429780,829080,615050,397900,722960,302670,588120,544840,602770,665360,451600,1782210,832680,447820,672490,346930,665550,707590,274940,684130,663920,278970,506730,401190,667530,374670,417860,530630,744760,227300,1092140,800280,519870,391240,364640,486780,546170,1086410,571740,431240,369990,914260,1520380,581740,679990,433530,269210,374280,457960,560080,348670,363360,513510,518150,728540,724430,844870,701760,794600,634340,1635450,546930,949000,420900,534210,516940,299740,749830,323850,4920,696400,471550,885570,643270,578080,757130,1986390,238960,1521580,321360,508710,774861,338170,764030,656610,1280770,517710,328070,574180,663690,270450,843660,1070330,366440,464350,705710,496960,684580,530300,485610,528970,690530,244850,207140,454580,381250,250820,264710,418030,321400,440730,437220,757160,859700,676340,654700,391460,562260,431960,230410,709870,308080,424370,657730,552990,625340,263920,914160,714910,722670,840140,1575870]

# Get the history of a single game from steamcharts.com
# Simulate a browser request to get the data
# Add a cookie to the request to avoid getting a 403 error
# The request url should be like this https://steamcharts.com/app/gameid/chart-data.json where gameid is the id of the game

def print_full(x):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    pd.set_option('display.float_format', '{:20,.2f}'.format)
    print(x)
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.float_format')

def get_steamcharts_history(gameid):
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
        print(response.text)
        return []
    return response.json()

# Get all the games names and corresponding ids from the steam api
#http://api.steampowered.com/ISteamApps/GetAppList/v0002/

def get_steam_app_list():
    url = 'http://api.steampowered.com/ISteamApps/GetAppList/v0002/'
    response = requests.get(url)
    if response.status_code != 200:
        print(response.status_code)
        print(response.text)
        return []
    return response.json()

#make a dictionary with the gameid as the key and the game name as the value
game_name_list = get_steam_app_list()
game_name_list = game_name_list['applist']['apps']
game_name_list = {game['appid']: game['name'] for game in game_name_list}

# Get the history of all games. It will be structured as a list of lists composed of [timestamp, players]
# Then join all the games history into a single pandas dataframe with the gameid as the column name and the timestamp as the index
# The lists will not have the same length because the games have been released at different times

all_games_history = pd.DataFrame()

for gameid in all_games:
    game_history = get_steamcharts_history(gameid)
    # For the gameid get the game name from the dictionary
    try:
        game_name = game_name_list[gameid]
    except KeyError:
        game_name = gameid

    if game_history:
        game_history = pd.DataFrame(game_history, columns=['timestamp', game_name])
        game_history = game_history.set_index('timestamp')
        all_games_history = pd.concat([all_games_history, game_history], axis=1)

# Convert the timestamp column to a datetime object from milliseconds
all_games_history.index = pd.to_datetime(all_games_history.index, unit='ms')

# Make sure the timestamp entries are one day apart. Resample the dataframe to have one entry per day
all_games_history = all_games_history.resample('M').mean()

# On the rows where there are no data, but both the previous and next rows have data, fill the missing data with the average of the previous and next values
all_games_history = all_games_history.interpolate(method='linear', axis=0)

# On the rows where there are no data, but the previous row has data, fill the missing data with the previous value
#all_games_history = all_games_history.fillna(method='ffill')

# Replace every gameid column with a moving average of the last 5 values
all_games_history = all_games_history.rolling(5).mean()

# Convert every valid value to an integer on the columns with the gameid
for gameid in all_games_history:
    all_games_history[gameid] = all_games_history[gameid].apply(lambda x: int(x) if not pd.isnull(x) else x)


for gameid in all_games_history:
    #get the column with the gameid
    column = all_games_history[gameid]

    print(f"{gameid} : {column.notna().sum()}")


print(all_games_history)

# Create 4 dataframes with the same structure of the all_games_history dataframe
# The four dataframes should be called:
# all_games_history_100
# all_games_history_1000
# all_games_history_10000
# all_games_history_100000

# To each of these dataframe add an index column called timestamp with the same timestamps of the all_games_history dataframe

# Iterate over the gameid column and get the latest amount of players

# If the latest amount of players is over 100, add the column with that gameid to the all_games_history_100 dataframe
# If the latest amount of players is between 100 and 1000, add the column with that gameid to the all_games_history_1000 dataframe
# If the latest amount of players is between 1000 and 10000, add the column with that gameid to the all_games_history_10000 dataframe
# If the latest amount of players is over 10000, add the column with that gameid to the all_games_history_100000 dataframe

all_games_history_100 = pd.DataFrame()
all_games_history_1000 = pd.DataFrame()
all_games_history_10000 = pd.DataFrame()
all_games_history_100000 = pd.DataFrame()

for gameid in all_games_history:
    latest_players = all_games_history[gameid].iloc[-1]
    print(f"{gameid} : {latest_players}")

    if latest_players > 100000:
        all_games_history_100000[gameid] = all_games_history[gameid]
    elif latest_players > 10000:
        all_games_history_10000[gameid] = all_games_history[gameid]
    elif latest_players > 1000:
        all_games_history_1000[gameid] = all_games_history[gameid]
    elif latest_players > 100:
        all_games_history_100[gameid] = all_games_history[gameid]



# Plot the 4 dataframes in 4 different subplots with a logarithmic scale on the y axis and the timestamp as the x axis and a legend
# The title of each subplot should be the name of the variable containing the dataframe
# The x axis should be labeled with "Date"
# The y axis should be labeled with "Players"
# The x axis should be formatted as a date

fig, axs = plt.subplots(2, 2, figsize=(20, 10))
fig.suptitle('Steamcharts history')

axs[0, 0].set_title('all_games_history_100')
axs[0, 0].set_ylabel('Players')
axs[0, 0].set_xlabel('Date')
axs[0, 0].set_yscale('log')
axs[0, 0].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
axs[0, 0].plot(all_games_history_100)

axs[0, 1].set_title('all_games_history_1000')
axs[0, 1].set_ylabel('Players')
axs[0, 1].set_xlabel('Date')
axs[0, 1].set_yscale('log')
axs[0, 1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
axs[0, 1].plot(all_games_history_1000)

axs[1, 0].set_title('all_games_history_10000')
axs[1, 0].set_ylabel('Players')
axs[1, 0].set_xlabel('Date')
axs[1, 0].set_yscale('log')
axs[1, 0].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
axs[1, 0].plot(all_games_history_10000)

axs[1, 1].set_title('all_games_history_100000')
axs[1, 1].set_ylabel('Players')
axs[1, 1].set_xlabel('Date')
axs[1, 1].set_yscale('log')
axs[1, 1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
axs[1, 1].plot(all_games_history_100000)

#Add a legend for every plot
axs[0, 0].legend(all_games_history_100.columns, loc='upper left')
axs[0, 1].legend(all_games_history_1000.columns, loc='upper left')
axs[1, 0].legend(all_games_history_10000.columns, loc='upper left')
axs[1, 1].legend(all_games_history_100000.columns, loc='upper left')


plt.show()

print("Done")