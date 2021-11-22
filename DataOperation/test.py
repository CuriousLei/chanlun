# -*- coding: utf-8 -*-
"""
Created on Fri Sep 17 16:44:41 2021

@author: leida
"""
import time
import datetime
"""数据采集模块"""
#import tushare as ts
#import requests
#import time
#import pandas as pd
#import sqlite3
#import json
#from pathlib import Path
#from .jukuan import Data
import sys
import pymysql
#TOKEN_PATH = Path(__file__).parent.parent.joinpath("DataBase", "token.txt")  # 用户密钥位置


#dataBase = Path(__file__).parent.parent.joinpath("DataBase", "stockData.db")
#Connect = sqlite3.connect(dataBase)
# sell_data = {'type':'S1', 'price':'24.1', 'time':'2021-9-17'}
# df_sell_data = pd.DataFrame(data=sell_data,index = [1])
# df_sell_data.to_sql('buy_sell_point_history', Connect, if_exists="append", index=True)
print(sys.version)
# import datetime
from MysqlUtils import MysqlUtils
db=MysqlUtils('127.0.0.1', 3307, 'root', '123456', 'chanlun', 'utf8')
# stock_id = '65565'
# level = '1d'
# time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#sql='INSERT INTO cl_point_result(stock_id, level, point, type, evaluation_time, valid, invalid_time, open, close, high, low) VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')'%(stock_id,level,time)
#print(sql)
#db.insert(sql)
db = pymysql.Connect(
    host='123.56.222.52',
    port=3306,
    user='root',
    passwd='5207158',
    db='chanlun',
    charset='utf8'
)
cursor = db.cursor()
records = None
try:
    cursor.execute("""select * from cl_point_result where stock_id=%s and level=%s and type=%s""", ('689009.XSHG', '1分钟', 'B1'))
    records = cursor.fetchone()
except:
    print("查询失败!")
print(records[3])
date = records[3]

print(date.strftime("%Y-%m-%d %H:%M"))
# time.strftime("%H:%M", records[3])
# regList=db.get_one('select * from cl_point_result')
# str = '2023003'
# print(str[0:-3])
# # print(regList[0])
#
# a = [1,2,3,4,5]
# print(a[-1])
# print(a[-3:-1])
# print(time.time())