import pandas as pd
import numpy as np
from tqdm import trange

# df_a=pd.read_csv('tracking_week_1.csv')
# df_b=pd.read_csv('plays.csv')
# #使用merge函数基于AB列连接A和B，并将B的C列拼接到A中
# result = df_a.merge(df_b[['gameId', 'playId', 'ballCarrierId','possessionTeam','defensiveTeam']], on=['gameId', 'playId'], how='left')
# result.to_csv('11.csv')
df=pd.read_csv('11.csv')
# 创建字典来存储相同的['gameId','playId']对应的ball_x和ball_y
ball_coordinates = {}

# 计算s_strain列
def calculate_s_strain(row):
    key = (row['gameId'], row['playId'])
    
    if key in ball_coordinates:
        ball_x, ball_y = ball_coordinates[key]
    else:
        ball_row = df[(df['gameId'] == row['gameId']) & 
                      (df['playId'] == row['playId']) &
                      (df['nflId'] == round(row['ballCarrierId']))]
        if not ball_row.empty:
            ball_x = ball_row['x'].iloc[0]
            ball_y = ball_row['y'].iloc[0]
            ball_coordinates[key] = (ball_x, ball_y)
        else:
            return None

    s_strain = np.sqrt((row['x'] - ball_x)**2 + (row['y'] - ball_y)**2)
    return s_strain

df['s_strain'] = df.apply(calculate_s_strain, axis=1)
#df.to_csv('111.csv')



def calculate_strain(s_strain, s_strain_frame_1):
    return (s_strain - s_strain_frame_1) / (0.1 * s_strain)

for index in trange(len(df)):
    if not df['nflId'].shift(1).isna().iloc[index] and df['nflId'].iloc[index] == df['nflId'].shift(1).iloc[index]:
        df.at[index, 'strain'] = calculate_strain(df['s_strain'].iloc[index], df['s_strain'].shift(1).iloc[index])
    else:
        df.at[index, 'strain'] = 0
