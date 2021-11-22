import pymysql
from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime
# mysqlCon = create_engine('mysql+pymysql://root:123456@127.0.0.1:3307/chanlun?charset=utf8')
#
# rawData = pd.read_sql("select * from 600809_XSHG_1d", mysqlCon)
# print(rawData)
# print(type(rawData))
# end_date = pd.to_datetime(datetime.now())
# print(type(end_date.strftime("%Y-%m-%d")))
# print(end_date.strftime("%Y-%m-%d"))
# date = '2021-10-18'
# print(type(date))
# print(time.strftime('%Y-%m-%d', time.localtime()))
# print(type(time.strftime('%Y-%m-%d', time.localtime())))
arr = [1,2,3,4,5]
print(arr[5:])
str = '1234567'
print(str[:6])
#print(arr[6])