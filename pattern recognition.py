import talib
import os
import pandas as pd
import numpy as np
from snowflake import connector


# PASSWORD = os.getenv("SNOWSQL_PASSWORD")
# ACCOUNT = os.getenv("SNOWSQL_ACCOUNT")
# USER = os.getenv("SNOWSQL_USER")
# print(PASSWORD, ACCOUNT, USER)

# conn = connector.connect(user = USER, password = PASSWORD, account = ACCOUNT)
# cs = conn.cursor()
# cs.execute("USE DATABASE PRICE_PATTERN_DETECTION")

# cs.execute("SELECT * FROM abc")
# df = cs.fetch_pandas_all()
# df.columns = df.columns.str.lower()
# print(df.head())


# df =  pd.read_csv(r"c:\Users\Owner\OneDrive\Documents\VS Code\abc_data_1d.csv")
# df["ADX"] = talib.ADX(df["high"], df["low"], df["close"], timeperiod=14)
# df["PPO"] = talib.PPO(df["close"], fastperiod=12, slowperiod=26, matype=0)
# df = df.drop(columns= ['open', 'high', 'low'], axis =1)
# df = df.dropna()
# #df.to_csv('adx_ppo.csv')
# df["prev_ADX"] = df["ADX"].shift()
# df["prev_PPO"] = df["PPO"].shift()
# df["prev_price"] = df["close"].shift()
# df["prev_time"] = df["time"].shift()
# df['ADX_chg'] = np.where(df['ADX'] >= df['prev_ADX'], 1, 0)
# df['PPO_chg'] = np.where(df['PPO'] >= df['prev_PPO'], 1, 0)
# df['price_chg'] = np.where(df['close'] >= df['prev_price'], 1, 0)


def detect_pattern(df):
    timestamps = []
    adx_decreasing = False
    ppo_increasing = False
    adx_was_decreasing = False
    ppo_was_increasing = False  
    #skip_rows = False

    for index, row in df.iterrows():
        adx_was_decreasing = adx_decreasing
        ppo_was_increasing = ppo_increasing
        
        
        timestamp = row['prev_time']
        adx = row['ADX']
        prev_adx = row['prev_ADX']
        ppo = row['PPO']
        prev_ppo = row['prev_PPO']


        adx_decreasing = adx < prev_adx
        ppo_increasing = ppo > prev_ppo

        adx_mountain = not adx_was_decreasing and adx_decreasing
        ppo_valley = not ppo_was_increasing and ppo_increasing
        
        
        if adx_mountain and ppo_valley:
            timestamps.append(timestamp)
            
        '''if skip_rows:
            if adx > prev_adx and ppo < prev_ppo:
                skip_rows = False

        if not skip_rows:
            if adx < prev_adx and ppo > prev_ppo:
                adx_decreasing = True
                ppo_increasing = True
                timestamps.append(timestamp)
                skip_rows = True'''

    return timestamps

def process_1h_files():
    data_files = [file for file in os.listdir() if file.endswith("data_1h.csv")]
    results1h = []

    for file in data_files:
        df = pd.read_csv(file)
        df["ADX"] = talib.ADX(df["high"], df["low"], df["close"], timeperiod=14)
        df["PPO"] = talib.PPO(df["close"], fastperiod=12, slowperiod=26, matype=0)
        df = df.drop(columns=['open', 'high', 'low'], axis=1)
        df = df.dropna()

        df["prev_ADX"] = df["ADX"].shift()
        df["prev_PPO"] = df["PPO"].shift()
        df["prev_price"] = df["close"].shift()
        df["prev_time"] = df["time"].shift()
        df['ADX_chg'] = np.where(df['ADX'] >= df['prev_ADX'], 1, 0)
        df['PPO_chg'] = np.where(df['PPO'] >= df['prev_PPO'], 1, 0)
        df['price_chg'] = np.where(df['close'] >= df['prev_price'], 1, 0)

        timestamps = detect_pattern(df)
        next_row_price_chg_values = []
        
        for timestamp in timestamps:
            row_index = df.index[df['time'] == timestamp]
            if len(row_index) > 0 and row_index[0] + 1 < len(df):
                next_row_price_chg = df.at[row_index[0] + 1, 'price_chg']
                next_row_price_chg_values.append(next_row_price_chg)
            else:
                next_row_price_chg_values.append(None)

        results1h.append((file, timestamps, next_row_price_chg_values))

    return results1h

def process_1m_files():
    data_files = [file for file in os.listdir() if file.endswith("data_1m.csv")]
    results1m = []

    for file in data_files:
        df = pd.read_csv(file)
        df["ADX"] = talib.ADX(df["high"], df["low"], df["close"], timeperiod=14)
        df["PPO"] = talib.PPO(df["close"], fastperiod=12, slowperiod=26, matype=0)
        df = df.drop(columns=['open', 'high', 'low'], axis=1)
        df = df.dropna()

        df["prev_ADX"] = df["ADX"].shift()
        df["prev_PPO"] = df["PPO"].shift()
        df["prev_price"] = df["close"].shift()
        df["prev_time"] = df["time"].shift()
        df['ADX_chg'] = np.where(df['ADX'] >= df['prev_ADX'], 1, 0)
        df['PPO_chg'] = np.where(df['PPO'] >= df['prev_PPO'], 1, 0)
        df['price_chg'] = np.where(df['close'] >= df['prev_price'], 1, 0)

        timestamps = detect_pattern(df)
        next_row_price_chg_values = []
        
        for timestamp in timestamps:
            row_index = df.index[df['time'] == timestamp]
            if len(row_index) > 0 and row_index[0] + 1 < len(df):
                next_row_price_chg = df.at[row_index[0] + 1, 'price_chg']
                next_row_price_chg_values.append(next_row_price_chg)
            else:
                next_row_price_chg_values.append(None)
                
        results1m.append((file, timestamps, next_row_price_chg_values))

    return results1m

def process_1d_files():
    data_files = [file for file in os.listdir() if file.endswith("data_1d.csv")]
    results1d = []

    for file in data_files:
        df1 = pd.read_csv(file)
        df = pd.read_csv(file)
        df["ADX"] = talib.ADX(df["high"], df["low"], df["close"], timeperiod=14)
        df["PPO"] = talib.PPO(df["close"], fastperiod=12, slowperiod=26, matype=0)
        df = df.drop(columns=['open', 'high', 'low'], axis=1)
        df = df.dropna()

        df["prev_ADX"] = df["ADX"].shift()
        df["prev_PPO"] = df["PPO"].shift()
        df["prev_price"] = df["close"].shift()
        df["prev_time"] = df["time"].shift()
        df['ADX_chg'] = np.where(df['ADX'] >= df['prev_ADX'], 1, 0)
        df['PPO_chg'] = np.where(df['PPO'] >= df['prev_PPO'], 1, 0)
        df['price_chg'] = np.where(df['close'] >= df['prev_price'], 1, 0)

        timestamps = detect_pattern(df)
        next_row_price_chg_values = []
        
        for timestamp in timestamps:
            row_index = df.index[df['time'] == timestamp]

            if len(row_index) > 0 and row_index[0] + 1 < len(df1):
                next_row_price_chg = df.at[row_index[0] + 1, 'price_chg']
                next_row_price_chg_values.append(next_row_price_chg)
            else:
                next_row_price_chg_values.append(None)

        results1d.append((file, timestamps, next_row_price_chg_values))

    return results1d

output1m = process_1d_files()
output1h = process_1h_files()
output1d = process_1m_files()

for file, timestamps, next_row_price_chg in output1m:
    print("File:", file)
    print("Timestamps:", timestamps)
    print("Price Change:", next_row_price_chg)
    print()

for file, timestamps, next_row_price_chg in output1h:
    print("File:", file)
    print("Timestamps:", timestamps)
    print("Price Change:", next_row_price_chg)
    print()

for file, timestamps, next_row_price_chg in output1d:
    print("File:", file)
    print("Timestamps:", timestamps)
    print("Price Change:", next_row_price_chg)
    print()

# new = pd.DataFrame.from_dict(output1m)
# print(new)

# timestamps = pd.DataFrame(detect_pattern(df), columns=['time'])
# merged_df = pd.merge(df, timestamps, on='time')
# #merged_df = merged_df[merged_df['price_chg'] == 1]
# #merged_df.to_csv("C:/Users/Owner/OneDrive/Documents/VS Code/pattern_detect_fin_abc.csv")
# print(merged_df)
