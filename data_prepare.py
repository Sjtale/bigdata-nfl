import pandas as pd
import math
import numpy as np


def process(game_dir,players_dir,plays_dir,gameId,playId):
    game_data = pd.read_csv(game_dir)
    game_data=game_data[(game_data['gameId']==gameId)&(game_data['playId']==playId)]
    #add the colunm
    players=pd.read_csv(players_dir)
    plays=pd.read_csv(plays_dir)
    # add the official_position
    def get_position(name, players_df):
        player = players_df[players_df['displayName'] == name]
        if not player.empty:
            return player.iloc[0]['position']
        else:
            return None  # or some default value like 'Unknown'

    # Apply the function to each row in game_data to create the new column
    game_data['official_position'] = game_data['displayName'].apply(get_position, players_df=players)

    # add the is_carry_ball
    def check_ball_carrier(game_row, plays_df):
        # Find the play in the players DataFrame that matches the gameId and playId
        play = plays_df[(plays_df['gameId'] == game_row['gameId']) & (plays_df['playId'] == game_row['playId'])]
        # If the ballCarrierDisplayName matches the displayName, return 1, else return 0
        if not play.empty and game_row['displayName'] == play.iloc[0]['ballCarrierDisplayName']:
            return 1
        else:
            return 0

    # Apply the function to each row in game_data
    game_data['is_carry_ball'] = game_data.apply(check_ball_carrier, axis=1, plays_df=plays)


    def find_possession_team(game_row, plays_df):
        # Find the play in the players DataFrame that matches the gameId and playId
        play = plays_df[(plays_df['gameId'] == game_row['gameId']) & (plays_df['playId'] == game_row['playId'])]
        # If the play exists and contains the 'possessionTeam' column, return its value, else return None
        if not play.empty and 'possessionTeam' in play.columns:
            return play.iloc[0]['possessionTeam']
        else:
            return None

    # Apply the function to each row in game_data and store the result in 'possessionteam' column
    game_data['possessionteam'] = game_data.apply(find_possession_team, axis=1, plays_df=plays)

    # add pass_frame
    def assign_pass_frame(play_data, events_of_interest):
        # Group by playId to process each play separately
        grouped = play_data.groupby('playId')
        
        # For each play, find the frame where the event occurred and assign it to all rows of that play
        for name, group in grouped:
            # Find the frameId where the event occurred
            event_frame = group[group['event'].isin(events_of_interest)]['frameId'].min()
            # If an event frame is found, assign it to the pass_frame column for the entire group
            play_data['pass_frame'] = 1
            if not pd.isna(event_frame):
                play_data.loc[play_data['playId'] == name, 'pass_frame'] = event_frame
        return play_data
    # Define the events we are looking for
    events =  ['handoff','run','pass_forward','lateral','pass_forward','pass_arrived']
    game_data=assign_pass_frame(game_data,events)

    # add end_frame
    def assign_end_frame(play_data, events_of_interest):
        # Group by playId to process each play separately
        grouped = play_data.groupby('playId')
        
        # For each play, find the frame where the event occurred and assign it to all rows of that play
        for name, group in grouped:
            # Find the frameId where the event occurred
            event_frame = group[group['event'].isin(events_of_interest)]['frameId'].min()
            # If an event frame is found, assign it to the pass_frame column for the entire group
            play_data['end_frame'] = 60
            if not pd.isna(event_frame):
                play_data.loc[play_data['playId'] == name, 'end_frame'] = event_frame
        return play_data

    # Define the events we are looking for
    events =['out_of_bounds', 'tackle','touchdown','qb_sack','qb_slide']
    game_data=assign_end_frame(game_data,events)


    #add x-qb,y-qb
    def qb_coordinates(frame_group):
        # Find the QB in the frame
        qb_row = frame_group[frame_group['official_position'] == 'QB']
        if not qb_row.empty:
            # Get the QB's x and y coordinates
            x_qb, y_qb = qb_row.iloc[0]['x'], qb_row.iloc[0]['y']
            # Calculate the difference for each player in the frame
            frame_group['x_qb'] = frame_group['x'] - x_qb
            frame_group['y_qb'] = frame_group['y'] - y_qb
            frame_group['s_strain']=np.sqrt((frame_group['x'] - x_qb)**2 + (frame_group['y'] - y_qb)**2)
        return frame_group

    # Group by frameId and apply the function
    game_data = game_data.groupby('frameId').apply(qb_coordinates).reset_index(drop=True)

    # add the strain
    def calculate_player_strain(player_group):
        # Sort the player's data by frameId to ensure it's in chronological order
        player_group = player_group.sort_values(by='frameId')
        
        # Calculate strain for each player, but skip the first frame
        if len(player_group) > 1:
            # Calculate the difference between s_strain values of the current frame and the previous frame for the same player
            player_group['strain'] = (-((player_group['s_strain'] - player_group['s_strain'].shift(1)) / 0.1) / player_group['s_strain']).fillna(0)
        
        return player_group
    # Separate football data
    football_data = game_data[game_data['club'] == 'football']

    # Separate player data
    player_data = game_data[game_data['club'] != 'football']

    # Apply the strain calculation to player data
    player_data = player_data.groupby('nflId').apply(calculate_player_strain).reset_index(drop=True)

    # Recombine player data with football data
    game_data = pd.concat([player_data, football_data], ignore_index=True)

    def calculate_vector(frame_group):
        # Calculate the vector for each player in the frame
        for index, row in frame_group.iterrows():
            dir = row['dir']
            x = math.sin(math.radians(180 - dir))
            y = math.cos(math.radians(180 - dir))
            frame_group.at[index, 'vector_x'] = x
            frame_group.at[index, 'vector_y'] = y
        
        return frame_group

    # Group by frameId and apply the function
    game_data = game_data.groupby('frameId').apply(calculate_vector).reset_index(drop=True)
    # Function to calculate dot product
    def dot_product(vec1, vec2):
        return np.dot(vec1, vec2)

    # Function to check if a vector is valid (not NaN)
    def is_valid_vector(vector):
        return not np.isnan(vector).any()


    df = game_data

    # Add the 'BULLE' column to the dataframe
    df['delta_angle'] = 0

    # Identify the clubs of players who are carrying the ball
    carry_ball_clubs = df[df['is_carry_ball'] == 1]['club'].unique()



    # Processing each row for dot product calculations
    for index, row in df.iterrows():
        frame = row['frameId']
        player_vector = np.array([row['vector_x'], row['vector_y']])

        # Initialize default values for vectors
        current_vector = np.array([np.nan, np.nan])
        pre_vector = np.array([np.nan, np.nan])

        # Get current_vector for frameId == frame and is_carry_ball == 1
        current_vector_row = df[(df['frameId'] == frame) & (df['is_carry_ball'] == 1)]
        if not current_vector_row.empty:
            current_vector = np.array([current_vector_row.iloc[0]['vector_x'], current_vector_row.iloc[0]['vector_y']])

        # Get pre_vector for frameId == frame - 1 and is_carry_ball == 1
        pre_vector_row = df[(df['frameId'] == frame - 1) & (df['is_carry_ball'] == 1)]
        if not pre_vector_row.empty:
            pre_vector = np.array([pre_vector_row.iloc[0]['vector_x'], pre_vector_row.iloc[0]['vector_y']])

        # Check if both current and previous vectors are valid before calculating dot products
        if is_valid_vector(current_vector) and is_valid_vector(pre_vector) and is_valid_vector(player_vector):
            # Compute dot products
            dot_product_pre = dot_product(pre_vector, player_vector)
            dot_product_current = dot_product(current_vector, player_vector)

            # Set the current row's 'BULLE' as the difference between the two dot products
            df.at[index, 'delta_angle'] = dot_product_current - dot_product_pre
        else:
            # Set default value if vectors are not valid
            df.at[index, 'delta_angle'] = 0
    # For players in these clubs, set 'BULLE' to 0
    df.loc[df['club'].isin(carry_ball_clubs), 'delta_angle'] = 0
    # For players in these clubs, set 'BULLE' to 0
    df.loc[df['club'].isin(carry_ball_clubs), 'delta_angle'] = 0
    try:
        df['BULLE'] = df['strain'] * df['delta_angle'] + df['delta_angle'] + df['strain']
    except Exception as e:
        df_empty = pd.DataFrame()  # 创建一个空的DataFrame
        return df_empty

    # 1.get["pass_frame", 'end_frame']
    pass_frame = int(game_data['pass_frame'].values[0])  
    end_frame = int(game_data['end_frame'].values[0])  #
    game_data['BULLE'] = game_data.apply(lambda row: 0 if (int(row['frameId']) < pass_frame or int(row['frameId']) > end_frame) else row['BULLE'], axis=1)

    return game_data




