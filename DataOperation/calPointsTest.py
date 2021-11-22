# coding=utf-8
# 首次使用需要设置聚宽账户
from datetime import datetime
import json

import pandas as pd

import sys
import time
from datetime import timedelta
# set_token("18811616732", '123456leidaJQ')
from ETQTSys.DataOperation import MysqlUtils, freq_obj_map
from ETQTSys.czsc.data.jq import set_token
from ETQTSys.czsc.data.jq import get_kline
from ETQTSys.czsc.objects import RawBar
from ETQTSys.czsc.utils.jsonEncoder import BiJSONEncoder

set_token("18800166629", '123QWEasdZXC')
from ETQTSys.czsc.analyze import CZSC
from ETQTSys.czsc.signals import get_default_signals

db = MysqlUtils('127.0.0.1', 3307, 'root', '123456', 'chanlun', 'utf8')

freq_map = {
    "1min": "1分钟",
    "5min": "5分钟",
    "15min": "15分钟",
    "30min": "30分钟",
    "60min": "60分钟",
    "D": "日线",
    "W": "周线",
    "M": "月线",
}
freq_convert = {"1min": "1m", "5min": '5m', '15min': '15m',
                "30min": "30m", "60min": '60m', "D": "1d", "W": '1w', "M": "1M"}
point_type_convert = {'类一买': 'B1', '类二买': 'B2', '类三买': 'B3', '类一卖': 'S1', '类二卖': 'S2', '类三卖': 'S3'}
factor_categories = ['基础形态', '类买卖点']
# codes = ['600809', '600810', '600811', '600812', '600814', '600815', '600816', '600817', '600818', '600819',
#          '600820', '600821',
#          '600822', '600824']
codes = ['600816', '600817', '600818', '600819',
         '600820', '600821',
         '600822', '600824']

# codes = ['600807']
# codes = ['600818', '600819', '600820', '600821', '600822', '600824']

# f = open(file="out.txt", mode="a+", encoding='utf-8')


def calPointsIntoDB(symbol, freq, now, bars):
    # bars = get_kline(symbol, freq=freq, end_date=datetime.now(), count=2000)
    # bars = get_kline(symbol, freq=freq, end_date=now, count=2000)
    # print(bars)
    c = CZSC(bars, get_signals=get_default_signals)
    signals = c.signals
    bi_list = c.finished_bis
    freq_name = freq_map[freq]
    # print(json.dumps(bi_list[-13:], cls=BiJSONEncoder))
    for category in factor_categories:
        for i in range(7):
            key = freq_name + '_倒' + str(i + 1) + '笔_' + category
            factor = signals[key]
            save_point(bi_list[-i - 1], factor, freq_name, now)


def save_point(bi, value, level, now):
    """保存当前笔对应的买卖点

    :param bi: 笔
    :param value: 信号
    """
    stock_id = bi.symbol
    # point = bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S')
    point = bi.fx_b.dt
    factor_prefix = value.split('_', 1)[0]
    if factor_prefix not in point_type_convert:
        return
    point_type = point_type_convert[factor_prefix]
    high = bi.fx_b.high
    low = bi.fx_b.low

    record = db.get_one(
        'select * from cl_point_result where stock_id=\'%s\' and level=\'%s\' and point=\'%s\' limit 1' %
        (stock_id, level, point))
    # print(record)
    if not record:
        db.insert(
            'insert into cl_point_result (stock_id, point, type, level, evaluation_time, high, low) values(\'%s\', \'%s\', \'%s\',\'%s\',\'%s\', %s, %s)' % (
                stock_id, point, point_type, level, now, high, low))
    else:
        db.update(
            'update cl_point_result set stock_id=\'%s\', point=\'%s\', type=\'%s\', level=\'%s\', evaluation_time=\'%s\', high=%s, low=%s where id =%s' % (
                stock_id, point, point_type, level, now, high, low, record[0]))


def get_db_k_data(symbol, freq):
    freq = freq_convert[freq]
    table_name = symbol.replace('.', '_') + '_' + freq
    rows = db.get_all('select * from %s' % table_name)
    # 根据K线数据获取bars
    bars = []
    i = 1
    for row in rows:
        dt = datetime.strptime(row[0], "%Y-%m-%d") if isinstance(row[0], str) else row[0]
        if int(row[5]) > 0:
            bars.append(
                RawBar(symbol=symbol, dt=dt, id=i, freq=freq_obj_map[freq],
                       open=round(float(row[1]), 2),
                       close=round(float(row[2]), 2),
                       high=round(float(row[3]), 2),
                       low=round(float(row[4]), 2),
                       vol=int(row[5])))
            i = i + 1
    return bars


if __name__ == '__main__':

    for code in codes:
        stock_id = code + '.XSHG'
        freq = 'D'
        # calPointsIntoDB(stock_id, freq, '2021-11-10 15:00:00')
        # freq = '1min'

        # minute_list_1 = pd.date_range(start='2021-11-08 09:30:00', end='2021-11-08 15:00:00', freq="min").format(
        #     formatter=lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        # minute_list_2 = pd.date_range(start='2021-11-09 09:30:00', end='2021-11-09 15:00:00', freq="min").format(
        #     formatter=lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        # minute_list_3 = pd.date_range(start='2021-11-10 09:30:00', end='2021-11-10 15:00:00', freq="min").format(
        #     formatter=lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        # minute_list_4 = pd.date_range(start='2021-11-11 09:30:00', end='2021-11-11 15:00:00', freq="min").format(
        #     formatter=lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        # minute_list = minute_list_1 + minute_list_2 + minute_list_3 + minute_list_4

        minute_list = pd.date_range(start='2015-01-01 00:00:00', end='2021-11-12 00:00:00').format(
            formatter=lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        bars = get_db_k_data(stock_id, freq)
        for minute in minute_list:
            new_bars = [x for x in bars if x.dt <= datetime.strptime(minute, "%Y-%m-%d %H:%M:%S")]
            pos = len(new_bars) - 2000 if len(new_bars) - 2000 > 0 else 0
            new_bars = new_bars[pos:]
            print(stock_id, minute, len(new_bars))
            calPointsIntoDB(stock_id, freq, minute, new_bars)
