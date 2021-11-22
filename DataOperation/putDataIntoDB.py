# -*- coding: utf-8 -*-
"""
Created on Tue Sep 28 15:10:42 2021

@author: leida
"""

from jukuan import Data
import time
from datetime import datetime
import pandas as pd
import pymysql
from sqlalchemy import create_engine
source = Data()
source.refreshToken()
dbcon = create_engine('mysql+pymysql://root:123456@127.0.0.1:3307/chanlun?charset=utf8')
# codes = ['600807', '600810', '600811', '600812', '600814', '600815', '600816', '600817', '600818', '600819', '600820', '600821', '600822', '600824']
codes = ['600809']
for cur_code in codes:
# cur_code = '600808'
    end_date = pd.to_datetime(datetime.now()).strftime("%Y-%m-%d")
    jukuan_data = source.get_bars(cur_code+".XSHG", 4000, end_date, '1d')
    del jukuan_data['paused']
    del jukuan_data['high_limit']
    del jukuan_data['low_limit']
    del jukuan_data['avg']
    del jukuan_data['pre_close']
    # jukuan_data = source.get_bars("600809.XSHG", 100, '2021-10-18', '60m')
    jukuan_data.columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume', 'Money']
    jukuan_data.to_sql(cur_code+'_XSHG_1d', con=dbcon, if_exists='append', index=False)
# print(jukuan_data)
# print(type(jukuan_data))
# del jukuan_data['paused']
# del jukuan_data['high_limit']
# del jukuan_data['low_limit']
# del jukuan_data['avg']
# del jukuan_data['pre_close']
# jukuan_data.columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume', 'Money']
# # print(jukuan_data.columns.values.tolist())
# dbcon = create_engine('mysql+pymysql://root:5207158@123.56.222.52:3306/chanlun?charset=utf8')

