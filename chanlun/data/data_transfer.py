#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/11/4 23:09
# @Author  : Lei Da
# @File    : data_transfer.py
# @Software: PyCharm
from typing import List

import pandas as pd
import numpy as np

from chanlun.analyze import check_fx
from chanlun.config.mysql_config import MysqlConfig
from chanlun.objects import RawBar, NewBar, BI, Hub
from chanlun.utils.db_connector import MysqlUtils

db = MysqlUtils(MysqlConfig.host,
                MysqlConfig.port,
                MysqlConfig.user,
                MysqlConfig.password,
                MysqlConfig.dbname,
                MysqlConfig.coding)


def raw_bars_transfer(db_data, freq, symbol):
    """原生K线数据转化为RawBar列表

        :param db_data: 数据库中获取的原生K线数据
        :param freq: K线级别
        :param symbol: 股票代码
        :return:RawBar列表
    """
    bars = []
    if not db_data:
        return bars

    x = np.array(db_data)
    if x.ndim == 1:
        db_data = [db_data]

    i = -1
    for row in db_data:
        # row = ['date', 'open', 'close', 'high', 'low', 'volume', 'money']
        dt = pd.to_datetime(row[0])

        if int(row[5]) > 0:
            i += 1
            bars.append(RawBar(symbol=symbol, dt=dt, id=i, freq=freq,
                               open=round(float(row[1]), 2),
                               close=round(float(row[2]), 2),
                               high=round(float(row[3]), 2),
                               low=round(float(row[4]), 2),
                               vol=int(row[5])))
    return bars


def new_bars_transfer(db_data, freq, symbol):
    """缠K线数据转化为NewBar列表

        :param db_data: 数据库中获取的缠K线数据
        :param freq: K线级别
        :param symbol: 股票代码
        :return:NewBar列表
    """
    bars = []
    if not db_data:
        return bars

    x = np.array(db_data)
    if x.ndim == 1:
        db_data = [db_data]

    for row in db_data:
        # row = ['id', 'stock_id', 'level', 'datetime', 'open', 'close', 'high', 'low', 'volume', 'elements']
        dt = pd.to_datetime(row[3])

        # 查找包含的K线
        if not row[9]:
            inside_bars = None
        else:
            time_range = row[9].split(",")
            inside_kxs = db.get_all('select * from ' + symbol.replace('.', '_') + '_' + freq +
                                    ' where Date between \'' + time_range[0] + '\' and \'' + time_range[1] + '\'')
            inside_bars = raw_bars_transfer(inside_kxs, freq, symbol)

        if int(row[8]) > 0:
            bars.append(NewBar(symbol=symbol, dt=dt, id=row[0], freq=freq,
                               open=round(float(row[4]), 2),
                               close=round(float(row[5]), 2),
                               high=round(float(row[6]), 2),
                               low=round(float(row[7]), 2),
                               vol=int(row[8]),
                               elements=inside_bars))
    return bars


def fxs_transfer(db_data, freq, symbol):
    """分型数据转化为FX列表

        :param db_data: 数据库中获取的分型数据
        :param freq: K线级别
        :param symbol: 股票代码
        :return:FX列表
    """
    fxs = []
    if not db_data:
        return fxs

    x = np.array(db_data)
    if x.ndim == 1:
        db_data = [db_data]

    for raw_fx in db_data:
        k1_id = raw_fx[5]
        k2_id = raw_fx[6]
        k3_id = raw_fx[7]
        three_bars = new_bars_transfer(
            db.get_all('select * from cl_kx where id=\'%s\' or id=\'%s\' or id=\'%s\' ' % (k1_id, k2_id, k3_id)), freq,
            symbol)
        if not three_bars or len(three_bars) != 3:
            continue
        # 根据三根K线计算分型
        fx = check_fx(three_bars[0], three_bars[1], three_bars[2])
        if not fx:
            print(three_bars)
            continue
        fx.id = raw_fx[0]
        fxs.append(fx)

    return fxs


def bi_transfer(db_data, freq, symbol):
    """数据库笔数据转化为BI列表

        :param db_data: 数据库中获取的分型数据
        :param freq: K线级别
        :param symbol: 股票代码
        :return: BI列表
    """
    bi_list = []
    if not db_data:
        return bi_list

    x = np.array(db_data)
    if x.ndim == 1:
        db_data = [db_data]

    for raw_bi in db_data:
        fx_a_id = raw_bi[3]
        fx_b_id = raw_bi[4]
        if fx_a_id == -1 or fx_b_id == -1:
            raise ValueError
        direction = raw_bi[5]
        start_dt = raw_bi[6]
        end_dt = raw_bi[7]
        high = raw_bi[9]
        low = raw_bi[10]
        # 获取内部分型列表
        raw_fxs = db.get_all(
            'select * from cl_shape where stock_id=\'%s\' and level=\'%s\' and id>=\'%s\' and id<=\'%s\'' % (
                symbol, freq, fx_a_id, fx_b_id))
        fxs = fxs_transfer(raw_fxs, freq, symbol)
        # 获取内部K线列表
        raw_kx = db.get_all(
            'select * from cl_kx where stock_id=\'%s\' and level=\'%s\' and datetime>=\'%s\' and datetime<=\'%s\'' % (
                symbol, freq, start_dt, end_dt))
        bars = new_bars_transfer(raw_kx, freq, symbol)

        fx_a = fxs[0]
        fx_b = fxs[-1]
        power_price = round(abs(fx_b.fx - fx_a.fx), 2)

        bi = BI(symbol=symbol, freq=fx_a.freq, id=raw_bi[0], direction=direction, fx_a=fx_a, fx_b=fx_b, fxs=fxs[1:-1],
                high=high,
                low=low, power=power_price, bars=bars)

        bi_list.append(bi)

    return bi_list


def hub_transfer(db_data, freq, symbol):
    """数据库中枢数据转化为hub列表

        :param db_data: 数据库中获取的中枢数据
        :param freq: K线级别
        :param symbol: 股票代码
        :return: hub列表
    """
    hub_list = []
    if not db_data:
        return hub_list

    x = np.array(db_data)
    if x.ndim == 1:
        db_data = [db_data]

    for raw_hub in db_data:
        start_time = raw_hub[3].strftime('%Y-%m-%d %H:%M:%S')
        end_time = raw_hub[4].strftime('%Y-%m-%d %H:%M:%S')

        raw_bi_list = db.get_all(
            'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and start_datetime>=\'%s\' and '
            'end_datetime<=\'%s\'' % (
                symbol, freq, start_time, end_time))
        bi_list = bi_transfer(raw_bi_list, freq, symbol)

        # 获取进入段和离开段
        entry_id = raw_hub[10]
        leave_id = raw_hub[11]
        entry = bi_transfer(db.get_one(
            'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and id=%s' % (
                symbol, freq, entry_id)), freq, symbol)[0] if entry_id != -1 else None
        leave = bi_transfer(db.get_one(
            'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and id=%s' % (
                symbol, freq, leave_id)), freq, symbol)[0] if leave_id != -1 else None

        hub = Hub(id=raw_hub[0], symbol=symbol, freq=freq, ZG=raw_hub[5], ZD=raw_hub[6], GG=raw_hub[7], DD=raw_hub[8],
                  entry=entry, leave=leave, elements=bi_list[::2])

        hub_list.append(hub)
    return hub_list
