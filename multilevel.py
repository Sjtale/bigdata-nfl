import pandas as pd
from tqdm import tqdm  # 导入tqdm库
from data_prepare import process
players_csv = 'players.csv'  
plays_csv = 'plays.csv'     
game_csv = 'tracking_week_9.csv'  
result = pd.read_csv('result.csv')  # columns=['playerName', 'club', 'position', 'BULLE']
tracking_data = pd.read_csv('tracking_week_9.csv')
tracking_data_game_play_id = tracking_data[['gameId', 'playId']].drop_duplicates()
tracking_data_game_play_id = tracking_data_game_play_id.values.tolist()
# 使用tqdm创建一个进度条
for play in tqdm(tracking_data_game_play_id):
    gameId = play[0]
    playId = play[1]
    try:
     play_data = process(game_csv, players_csv, plays_csv, gameId, playId)
    except:
        continue
    for index, row in play_data.iterrows():
        s_strain = float(row['s_strain'])
        if s_strain < 5:
            player_name = row['displayName']
            bulle = row['BULLE']
            club = row['club']
            position = row['official_position']
            if bulle <= 0:
                bulle = 0
                
            # 检查player_name是否已经在结果DataFrame中
            player_exists = result['playerName'] == player_name
            
            if any(player_exists):
                # 如果player_name已经存在，将bulle添加到对应行
                result.loc[player_exists, 'BULLE'] += bulle
            else:
                # 如果player_name不存在，创建一个新行并添加数据
                new_row = pd.DataFrame([[player_name, club, position, bulle]], columns=['playerName', 'club', 'position', 'BULLE'])
                result = pd.concat([result, new_row], ignore_index=True)

result.to_csv('result_week9.csv')