from alpha_vantage.timeseries import TimeSeries
import time
import datetime
from datetime import timedelta
import os
import numpy as np
import pandas as pd


def daterange(start_date, end_date, offset=1):
    for n in range(int((end_date - start_date).days) + offset):
        yield start_date + timedelta(n)


def read_settings():
    if os.path.isfile('.settings'):
        settings = open(".settings", "r")
        api_token, db_name, stocks = settings.read().split('\n')
        stocks = stocks.split(',')
        settings.close()
    else:
        api_token = input('Enter API key:')
        db_name = input('Enter Database name:')
        stocks = []
        print('For exit type "qq"')
        while True:
            stock = input('Enter stock name:')
            if stock == 'qq':
                break
            stocks.append(stock)
        settings = open(".settings", "w")
        settings.writelines([api_token, '\n', f'{db_name}.db', '\n', ','.join(stocks)])
        settings.close()
    return api_token, db_name, stocks


def gen_metric(db, metrics_df_dict, stock, min_day, max_day, j):
    df = pd.read_sql(f'SELECT * from {stock}_RAW;', db)
    df['day'] = df['date'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date())
    df['date'] = df['date'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    for d in daterange(min_day, max_day):
        if len(df.loc[df['day'] == d]) > 0:
            offset = df.loc[df['day'] == d].index[0]
            sum_vol = np.sum(df.loc[df['day'] == d]['5. volume'])
            open_val = df.loc[df['day'] == d].loc[0 + offset]['1. open']
            close_val = df.loc[df['day'] == d].loc[len(df.loc[df['day'] == d]) - 1 + offset]['4. close']
            diff = (open_val / close_val - 1) * 100
            time_max_vol = df.loc[df['day'] == d].loc[np.argmax(df.loc[df['day'] == d]['5. volume']) + offset][
                'date']
            time_max_val = df.loc[df['day'] == d].loc[np.argmax(df.loc[df['day'] == d]['2. high']) + offset][
                'date']
            time_min_val = df.loc[df['day'] == d].loc[np.argmax(df.loc[df['day'] == d]['3. low']) + offset][
                'date']
            row = [j, stock, sum_vol, open_val, close_val, diff, time_max_vol, time_max_val, time_min_val]
            for n, v in zip(metrics_df_dict, row):
                metrics_df_dict[n].append(v)


def download_all_data(api_key, symbol, from_=None, to=str(datetime.datetime.now())):
    intervals = ['1min']
    ts = TimeSeries(key=api_key, output_format='pandas')
    init_data_list = []
    i = 0
    while i < len(intervals):
        try:
            data, meta_data = ts.get_intraday(symbol=symbol, interval=intervals[i], outputsize='full')
            if from_ is None:
                init_data_list.append(data[:to])
            else:
                init_data_list.append(data[from_:to])
            i += 1
        except ValueError:
            print('Waiting because call frequency is 5 calls per minute')
            time.sleep(62)
    init_data_df = init_data_list[0]
    init_data_df = init_data_df.sort_index()
    init_data_df = init_data_df.drop_duplicates()
    return init_data_df
