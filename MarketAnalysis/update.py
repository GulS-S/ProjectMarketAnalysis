from utils import read_settings, download_all_data, gen_metric
from datetime import timedelta
import datetime
import pandas as pd
import sqlite3 as sl


def update_data():
    api_token, db_name, stocks = read_settings()
    db = sl.connect(db_name)
    all_data = pd.read_sql('SELECT * FROM METRICS;', db)
    all_data['time_when_max_volume'] = all_data['time_when_max_volume'].map(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    all_data['time_when_max_value'] = all_data['time_when_max_value'].map(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    all_data['time_when_min_value'] = all_data['time_when_min_value'].map(
        lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    metrics_df_dict = {'category_id': [], 'name': [], 'sum_volume': [], 'open_val': [],
                       'close_val': [], 'diff_percentage': [], 'time_when_max_volume': [],
                       'time_when_max_value': [], 'time_when_min_value': []}
    for j, stock in enumerate(stocks):
        last_upd_moment = datetime.datetime.strptime(pd.read_sql(
            f"SELECT MAX(strftime('%Y-%m-%d %H:%M:%S', date)) FROM {stock}_RAW;", db).values[0][0],
                                                     '%Y-%m-%d %H:%M:%S') + timedelta(1 / (24 * 60))
        new_data_df = download_all_data(api_token, stock, str(last_upd_moment))
        new_data_df.to_sql(f'{stock}_RAW', db, if_exists='append')
        min_day = last_upd_moment.date()
        max_day = datetime.datetime.strptime(pd.read_sql(f"SELECT MAX(strftime('%Y %m %d', date)) "
                                                         f"FROM {stock}_RAW;", db).values[0][0], '%Y %m %d').date()
        gen_metric(db, metrics_df_dict, stock, min_day, max_day, j)

        all_data['time'] = all_data['time_when_min_value'].map(
            lambda x: x.date())

        all_data = all_data.drop(all_data.loc[(all_data['time'] == min_day) & (all_data['name'] == stock)].index)
        all_data = all_data.drop(['time'], axis=1)
    metrics_df = pd.DataFrame(data=metrics_df_dict)
    metrics_df = pd.concat([all_data, metrics_df])
    metrics_df.to_sql('METRICS', db, if_exists='replace', index=False)


update_data()
