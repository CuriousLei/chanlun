# -*- coding: utf-8 -*-
"""
Created on Fri Sep 24 20:27:18 2021

@author: leida
"""

from jukuan import Data
import pymysql
from sqlalchemy import create_engine
import pandas as pd
source = Data()
source.refreshToken()

#print(source.get_bars("502050.XSHG", 20, '2015-12-31', '1m'))
#data = source.get_bars("600809.XSHG", 1000, '2021-09-26', '1m')
#print(data)
#for idx,item in data.iterrows():
#    print(idx)
#    print(item)
#将数据存入数据库
dbcon = create_engine('mysql+pymysql://root:123456@127.0.0.1:3307/chanlun?charset=utf8')
rawData = pd.read_sql('select * from 600809_XSHG_1m', dbcon) 
print(rawData)
#data.to_sql('600809.XSHG_1m', con=dbcon, if_exists='replace')
#dataBase = Path(__file__).parent.parent.joinpath("DataBase", "stockData.db")
#Connect = sqlite3.connect(dataBase)
#sell_data = {'type':'S1', 'price':'24.1', 'time':'2021-9-17'}
#df_sell_data = pd.DataFrame(data=sell_data,index = [1])
#df_sell_data.to_sql('buy_sell_point_history', Connect, if_exists="append", index=True)