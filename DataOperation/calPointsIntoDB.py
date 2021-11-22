#coding=utf-8
# 首次使用需要设置聚宽账户
import czsc
from czsc.data.jq import set_token
# set_token("18800166629", '123QWEasdZXC')
set_token("18811616732", '123456leidaJQ')
import sys
from datetime import datetime
from datetime import timedelta
from czsc.data.jq import get_kline
from czsc.analyze import CZSC
from czsc.signals import get_default_signals
import pymysql
import time
from jqdatasdk import *
from sqlalchemy import create_engine
import pandas as pd

assert czsc.__version__ == '0.7.9'
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
db = pymysql.Connect(
    host='123.56.222.52',
    port=3306,
    user='root',
    passwd='5207158',
    db='chanlun',
    charset='utf8'
)
def check_valid(stock_id, bi_list, freq):
    if not bi_list:
        return
    set_bi_time = set()
    now = datetime.now()
    start = now + timedelta(days=2000)
    end = now - timedelta(days=2000)
    for bi in bi_list:
        set_bi_time.add(bi.fx_a.dt)
        set_bi_time.add(bi.fx_b.dt)
        start = min(start, bi.fx_a.dt)
        start = min(start, bi.fx_b.dt)
        end = max(end, bi.fx_a.dt)
        end = max(end, bi.fx_b.dt)
    cursor = db.cursor()
    record = None
    print(start)
    print(end)
    try:
        cursor.execute("""select * from cl_point_result where stock_id=%s and level=%s and point between %s and %s""",
                       (stock_id, freq, start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")))
        record = cursor.fetchall()
    except:
        print("check_valid: 查询失败!")
    if not record:
        return
    for item in record:
        if not set_bi_time.__contains__(item):
            if not item[7]:
                try:
                    cursor.execute("""update cl_point_result set valid=0, invalid_time=%s where id=%s""",
                                   (now, item[0]))
                    db.commit()
                except:
                    print("check_valid: 更新失败!")

def calPointsIntoDB(symbol, freq):
    # bars = get_kline(symbol, freq=freq, end_date=datetime.now(), count=2000)
    bars = get_kline(symbol, freq=freq, end_date='2021-10-27 14:41:11', count=2000)
    # print(bars)
    c = CZSC(bars, get_signals=get_default_signals)
    signals = c.signals
    bi_list = c.finished_bis
    freq_name = freq_map[freq]
    # print(signals)

    for category in factor_categories:
        for i in range(7):
            key = freq_name+'_倒'+str(i+1)+'笔_'+category
            factor = signals[key]
            save_point(bi_list[-i-1], factor, freq_name)

def save_point(bi, value, level):
    """保存当前笔对应的买卖点

    :param bi: 笔
    :param value: 信号
    """
    stock_id = bi.symbol
    point = bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S')
    factor_prefix = value.split('_', 1)[0]
    if factor_prefix not in point_type_convert:
        return
    point_type = point_type_convert[factor_prefix]
    now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    high = bi.fx_b.high
    low = bi.fx_b.low

    print(level+'_'+point_type+'_'+point)
    return
    cursor = db.cursor()
    record = None
    try:
        cursor.execute("""select * from cl_point_result where stock_id=%s and level=%s and point=%s""", (stock_id, level, point))
        record = cursor.fetchone()
    except:
        print("查询失败!")

    # 重复数据也直接插入数据库
    try:
        cursor.execute(
            """insert into cl_point_result (stock_id, point, type, level, evaluation_time, high, low, valid) values(%s, %s, %s, %s, %s, %s, %s)""",
            (stock_id, point, point_type, level, now, high, low, 1))
        db.commit()
    except:
        print("插入失败!")
    # if record is None:
    #     try:
    #         cursor.execute("""insert into cl_point_result (stock_id, point, type, level, evaluation_time, high, low) values(%s, %s, %s, %s, %s, %s, %s)""",
    #                             (stock_id, point, point_type, level, now, high, low))
    #         db.commit()
    #     except:
    #         print("插入失败!")
    # else:
    #     try:
    #         cursor.execute("""update cl_point_result set stock_id=%s, point=%s, type=%s, level=%s, evaluation_time=%s, high=%s, low=%s where id =%s""",
    #                             (stock_id, point, point_type, level, now, high, low, record[0]))
    #         db.commit()
    #     except:
    #         print("修改失败!")

def crawDataIntoDB(stock_id, freq):
    auth('18800166629', '123QWEasdZXC')
    dbcon = create_engine('mysql+pymysql://root:5207158@123.56.222.52:3306/chanlun?charset=utf8')
    now = datetime.now()
    freq = freq_convert[freq]
    table_name = stock_id.replace('.', '_') + '_' + freq

    if len(pd.read_sql('show tables like "' + table_name + '"', dbcon)) == 1:
        last_datetime = dbcon.execute('select Date from '+table_name+' order by Date desc limit 1;')
        last_datetime = (last_datetime.fetchone()[0] + timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')

        data = get_price(stock_id, start_date=last_datetime, end_date=now, frequency=freq)
    else:
        data = get_price(stock_id, count=1000, end_date=now, frequency=freq)

    df = pd.DataFrame(data)
    df.columns = ['Open', 'Close', 'High', 'Low', 'Volume', 'Money']
    data.to_sql(table_name, con=dbcon, if_exists='append', index_label='Date')

def crawHistoryDataIntoDB(stock_id, freq):
    auth('18800166629', '123QWEasdZXC')
    dbcon = create_engine('mysql+pymysql://root:5207158@123.56.222.52:3306/chanlun?charset=utf8')
    now = datetime.now()
    freq = freq_convert[freq]
    table_name = stock_id.replace('.', '_') + '_' + freq


    data = get_price(stock_id, start_date='2015-09-02 09:00:00', end_date=now, frequency=freq)

    df = pd.DataFrame(data)
    df.columns = ['Open', 'Close', 'High', 'Low', 'Volume', 'Money']
    data.to_sql(table_name, con=dbcon, if_exists='replace', index_label='Date')
if __name__ == '__main__':
    stock_id = '600809.XSHG'
    freq = sys.argv[1]
    # 拉取历史数据
    crawHistoryDataIntoDB(stock_id, '1min')
    # 通过聚宽拉取股票数据，存入数据库
    # crawDataIntoDB(stock_id, freq)
    # 计算买卖点并写入数据库

    # calPointsIntoDB(stock_id, freq)
