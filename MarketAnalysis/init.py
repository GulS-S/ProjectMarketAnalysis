import datetime
import pandas as pd
from utils import read_settings, download_all_data, gen_metric
import sqlite3 as sl


def verify_db():
    api_token, db_name, stocks = read_settings()
    db = sl.connect(db_name)
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [i[0] for i in cursor.fetchall()]
    for stock in stocks:
        if f'{stock}_RAW' not in tables:
            data = download_all_data(api_token, stock)
            data.to_sql(f'{stock}_RAW', db)
            tables.append(f'{stock}_RAW')
    if 'METRICS' not in tables:
        metrics_df_dict = {'category_id': [], 'name': [], 'sum_volume': [], 'open_val': [],
                           'close_val': [], 'diff_percentage': [], 'time_when_max_volume': [],
                           'time_when_max_value': [], 'time_when_min_value': []}
        for j, stock in enumerate(stocks):
            min_day = datetime.datetime.strptime(pd.read_sql(f"SELECT MIN(strftime('%Y %m %d', date)) "
                                                             f"FROM {stock}_RAW;", db).values[0][0], '%Y %m %d').date()
            max_day = datetime.datetime.strptime(pd.read_sql(f"SELECT MAX(strftime('%Y %m %d', date)) "
                                                             f"FROM {stock}_RAW;", db).values[0][0], '%Y %m %d').date()
            gen_metric(db, metrics_df_dict, stock, min_day, max_day, j)
        metrics_df = pd.DataFrame(data=metrics_df_dict)
        metrics_df.to_sql('METRICS', db, index=False)
