#coding=utf-8
import sys
import pandas as pd
from jqdatasdk import *
from sqlalchemy import create_engine
import datetime

def crawDataIntoDB(stock_id, freq):
    auth('18800166629', '123QWEasdZXC')
    dbcon = create_engine('mysql+pymysql://root:5207158@123.56.222.52:3306/chanlun?charset=utf8')
    table_name = stock_id.replace('.', '_') + '_' + freq

    if len(pd.read_sql('show tables like "' + table_name + '"', dbcon)) == 1:
        last_datetime = dbcon.execute('select Date from '+table_name+' order by Date desc limit 1;')
        last_datetime = (last_datetime.fetchone()[0] + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(last_datetime)
        print(now)
        data = get_price(stock_id, start_date=last_datetime, end_date=now, frequency=freq)
        print(data)
    else:
        data = get_price(stock_id, count=1000, end_date='2021-10-19 16:00:00', frequency=freq)

    df = pd.DataFrame(data)
    df.columns = ['Open', 'Close', 'High', 'Low', 'Volume', 'Money']
    data.to_sql(table_name, con=dbcon, if_exists='append', index_label='Date')
if __name__ == '__main__':
    freq = sys.argv[1]
    crawDataIntoDB('600809.XSHG', freq)