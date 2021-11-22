# -*- coding: utf-8 -*-
"""
Created on Fri Sep 17 17:24:55 2021

@author: leida
"""

import requests
import random
import requests, json
import sys
import pandas as pd
from io import StringIO
import multiprocessing
import numpy

lock = multiprocessing.Lock()
headers = {'Connection': 'close'}

class Data:
    token = None

    # symbol股票编号，scale数据的时间间隔，ma均值，datalen数据长度（最大1023）
    def __init__(self):
        self.url = "https://dataapi.joinquant.com/apis"
        # 获取调用凭证
        body = {
            "method": "get_token",
            # "mob": "18800166629",  # mob是申请JQData时所填写的手机号
            "mob": "18811616732",
            "pwd": '123456leidaJQ',
            # "pwd": "123QWEasdZXC",  # Password为聚宽官网登录密码，新申请用户默认为手机号后6位
        }

        if Data.token == None:
            response = requests.post(self.url, data=json.dumps(body), headers=headers)
            Data.token = response.text

        #print("初始化", Data.token)

        self.table = {}
        self.count = 0
        self.curStockPrice = 0

    def refreshToken(self):
        # 获取调用凭证
        body = {
            "method": "get_current_token",
            # "mob": "18800166629",  # mob是申请JQData时所填写的手机号
            # "pwd": "123QWEasdZXC",  # Password为聚宽官网登录密码，新申请用户默认为手机号后6位
            "mob": "18811616732",
            "pwd": '123456leidaJQ',
        }
        response = requests.post(self.url, data=json.dumps(body), headers=headers)
        # print("返回的token", response.text)
        Data.token = response.text

    def get_data(self, response):  # 数据处理函数,处理csv字符串函数
        '''格式化数据为DataFrame'''
        return pd.read_csv(StringIO(response.text))

    def get_bars(self, symbol, datalen, day, unit):
        response = None
        try:
            lock.acquire()
            self.refreshToken()
            body = {
                "method": "get_price",
                "token": Data.token,
                "code": symbol,
                "count": datalen,
                "unit": unit,
                "end_date": day,
                # "fq_ref_date": "2018-07-21"
            }
            response = requests.post(self.url, data=json.dumps(body), headers=headers)
        except Exception as ex:
            print(ex)
            print("获取数据异常")
        finally:
            lock.release()
            if response == None:
                return None
            else:
                return self.get_data(response)
