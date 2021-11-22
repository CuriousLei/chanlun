#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/11/2 15:41
# @Author  : Lei Da
# @File    : task.py
# @Software: PyCharm
from datetime import datetime
from typing import List

import pandas as pd
import numpy as np

from chanlun.analyze import remove_include, check_fxs, generate_bi, generate_hub
from chanlun.config.mysql_config import MysqlConfig
from chanlun.data.data_transfer import raw_bars_transfer, new_bars_transfer, fxs_transfer, bi_transfer, hub_transfer
from chanlun.objects import NewBar
from chanlun.utils.db_connector import MysqlUtils

db = MysqlUtils(MysqlConfig.host,
                MysqlConfig.port,
                MysqlConfig.user,
                MysqlConfig.password,
                MysqlConfig.dbname,
                MysqlConfig.coding)


def cal_kx(symbol, freq):
    """计算K线合并，并写入cl_kx数据库表

        :param symbol: 股票代码
        :param freq: K线级别
        :param arrived_kx: 新进的K线数据
    """

    # 读取数据库表cl_kx中倒数两根K线
    last_cl_kx = db.get_all(
        'select * from cl_kx where stock_id=\'' + symbol + '\' and level=\'' + freq + '\' order by id desc limit 2')
    bars_ubi = new_bars_transfer(last_cl_kx, freq, symbol)

    init_len = 0 if not last_cl_kx else len(last_cl_kx)

    # 读取新进K线
    if init_len == 0:
        raw_arrived_kx = db.get_all(
            'select * from %s' % (symbol.replace('.', '_') + '_' + freq))
    else:
        raw_arrived_kx = db.get_all(
            'select * from %s where Date > \'%s\'' % (symbol.replace('.', '_') + '_' + freq, bars_ubi[-1].dt))

    # 新进K线数据结构转化
    arrived_bars = raw_bars_transfer(raw_arrived_kx, freq, symbol)

    # 添加新的K线
    for bar in arrived_bars:
        if len(bars_ubi) < 2:
            bars_ubi.append(NewBar(symbol=bar.symbol, id=bar.id, freq=bar.freq, dt=bar.dt,
                                   open=bar.open, close=bar.close,
                                   high=bar.high, low=bar.low, vol=bar.vol, elements=[bar]))
        else:
            k1, k2 = bars_ubi[-2:]
            # 合并处理
            has_include, k3 = remove_include(k1, k2, bar)
            if has_include:
                bars_ubi[-1] = k3
            else:
                bars_ubi.append(k3)

    # 将合并后的K线插入数据库
    for i in range(init_len):
        elements_str = ''
        combined = 0
        if bars_ubi[i].elements and len(bars_ubi[i].elements) > 1:
            elements_str = bars_ubi[i].elements[0].dt.strftime('%Y-%m-%d %H:%M:%S') + ',' + \
                           bars_ubi[i].elements[-1].dt.strftime('%Y-%m-%d %H:%M:%S')
            combined = 1
        db.update(
            "update cl_kx set datetime=\'%s\', open=%s, close=%s, high=%s, low=%s, volume=%s, elements=\'%s\', "
            "combined=%s where id=%s" % (
                bars_ubi[i].dt.strftime('%Y-%m-%d %H:%M:%S'),
                bars_ubi[i].open,
                bars_ubi[i].close,
                bars_ubi[i].high,
                bars_ubi[i].low,
                bars_ubi[i].vol,
                elements_str,
                bars_ubi[i].id,
                combined
            ))
    for i in range(init_len, len(bars_ubi)):
        elements_str = ''
        combined = 0
        if len(bars_ubi[i].elements) > 1:
            elements_str = bars_ubi[i].elements[0].dt.strftime('%Y-%m-%d %H:%M:%S') + ',' + \
                           bars_ubi[i].elements[-1].dt.strftime('%Y-%m-%d %H:%M:%S')
            combined = 1
        db.insert(
            'insert into cl_kx(stock_id, level, datetime, open, close, high, low, volume, elements, combined) values('
            '\'%s\', \'%s\',\'%s\',%s,%s,%s,%s,%s,\'%s\',%s)' % (
                symbol,
                freq,
                bars_ubi[i].dt.strftime('%Y-%m-%d %H:%M:%S'),
                bars_ubi[i].open,
                bars_ubi[i].close,
                bars_ubi[i].high,
                bars_ubi[i].low,
                bars_ubi[i].vol,
                elements_str,
                combined
            ))


def cal_fx(symbol, freq):
    """计算分型，并写入cl_shape数据库表

        :param symbol: 股票代码
        :param freq: K线级别
        :param arrived_bars: 新进的已合并K线数据
    """
    # 读取最近一个分型
    raw_last_fx = db.get_one(
        'select * from cl_shape where stock_id=\'%s\' and level=\'%s\' order by datetime desc limit 1' % (
            symbol, freq))
    last_fxs = fxs_transfer(raw_last_fx, freq, symbol)
    last_fx = last_fxs[0] if last_fxs and len(last_fxs) > 0 else None

    first_k_id = last_fx.elements[0].id if last_fxs and len(last_fxs) > 0 else 0
    # 读取最近一个分型后的所有已合并K线(包括该分型内的K线)
    last_cl_kx = db.get_all(
        'select * from cl_kx where id>=%s and stock_id=\'%s\' and level=\'%s\'' % (
            first_k_id, symbol, freq))
    last_bars_ubi = new_bars_transfer(last_cl_kx, freq, symbol)
    # 合并已有K线和新进K线
    bars_ubi = last_bars_ubi

    # 计算分型
    fxs = check_fxs(bars_ubi)
    if not fxs or len(fxs) < 1:
        return

    start_idx = 1 if last_fx and fxs[0].dt == last_fx.dt else 0

    for i in range(start_idx, len(fxs)):
        fx = fxs[i]
        k1 = fx.elements[0]
        k2 = fx.elements[1]
        k3 = fx.elements[2]
        db.insert(
            'insert into cl_shape(stock_id, level, datetime, mark, k1, k2, k3, high, low) values(\'%s\',\'%s\','
            '\'%s\',\'%s\',%s,%s,%s,%s,%s)' % (
                symbol, freq, fx.dt.strftime('%Y-%m-%d %H:%M:%S'), fx.mark, k1.id, k2.id, k3.id, fx.high, fx.low))


def cal_bi(symbol, freq):
    """计算笔，并写入cl_strock数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    raw_last_bi_list = db.get_all(
        'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' order by id desc limit 2' % (symbol, freq))
    bi_list = bi_transfer(raw_last_bi_list, freq, symbol)
    bi_list.reverse()

    init_len = len(bi_list)

    if init_len == 0:
        arrived_bars = new_bars_transfer(
            db.get_all('select * from cl_kx where stock_id=\'%s\' and level=\'%s\'' % (symbol, freq)), freq,
            symbol)
    else:
        arrived_bars = new_bars_transfer(
            db.get_all('select * from cl_kx where stock_id=\'%s\' and level=\'%s\' and datetime>\'%s\'' % (
                symbol, freq, bi_list[-1].fx_b.dt)), freq, symbol)
    bars = []
    for bar in arrived_bars:
        bars.append(bar)
        bi_list, bars = generate_bi(bi_list, bars)

    for i in range(len(bi_list)):
        bi = bi_list[i]
        state = 'done' if i < len(bi_list) - 2 else 'undone'
        if bi.id >= 0:
            if state == 'done':
                done_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                db.update(
                    'update cl_stroke set fx_a=%s , fx_b=%s , direction=\'%s\' , start_datetime=\'%s\' , '
                    'end_datetime=\'%s\' , high=%s , low=%s , state=\'%s\' , done_time=\'%s\' where id=\'%s\'' % (
                        bi.fx_a.id, bi.fx_b.id, bi.direction, bi.fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'),
                        bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S'), bi.high, bi.low, state, done_time, bi.id))
            else:
                db.update(
                    'update cl_stroke set fx_a=%s , fx_b=%s , direction=\'%s\' , start_datetime=\'%s\' , '
                    'end_datetime=\'%s\' , high=%s , low=%s , state=\'%s\' where id=\'%s\'' % (
                        bi.fx_a.id, bi.fx_b.id, bi.direction, bi.fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'),
                        bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S'), bi.high, bi.low, state, bi.id))
        else:
            if state == 'done':
                done_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                db.insert(
                    'insert into cl_stroke(stock_id, level, fx_a, fx_b, direction, start_datetime, end_datetime, high, '
                    'low, state, done_time) values (\'%s\',\'%s\',%s, %s,\'%s\',\'%s\',\'%s\',%s,%s,\'%s\',\'%s\')' % (
                        symbol, freq, bi.fx_a.id, bi.fx_b.id, bi.direction, bi.fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'),
                        bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S'), bi.high, bi.low, state, done_time
                    ))
            else:
                db.insert(
                    'insert into cl_stroke(stock_id, level, fx_a, fx_b, direction, start_datetime, end_datetime, high, '
                    'low, state) values (\'%s\',\'%s\',%s, %s,\'%s\',\'%s\',\'%s\',%s,%s,\'%s\')' % (
                        symbol, freq, bi.fx_a.id, bi.fx_b.id, bi.direction, bi.fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'),
                        bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S'), bi.high, bi.low, state
                    ))


def cal_xd(symbol, freq):
    """计算线段，并写入cl_paragraph数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    return None


def cal_zh(symbol, freq, component_type):
    """计算中枢，并写入cl_omphalos数据库表

        :param symbol: 股票代码
        :param freq: K线级别
        :param component_type: 构件（bi/xd）
    """
    # 读取最近的中枢
    raw_last_hub = db.get_one(
        'select * from cl_omphalos where stock_id=\'%s\' and level=\'%s\' order by id desc limit 1' % (symbol, freq))
    hubs = hub_transfer(raw_last_hub, freq, symbol)

    # 获取最近中枢之后的笔或者线段
    if len(hubs) == 0:
        raw_arrived_bi_list = db.get_all(
            'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\'' % (
                symbol, freq))
    else:
        raw_arrived_bi_list = db.get_all(
            'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and start_datetime>\'%s\'' % (
                symbol, freq, hubs[-1].elements[-1].fx_b.dt))

    arrived_bi_list = bi_transfer(raw_arrived_bi_list, freq, symbol)

    # 生成中枢
    bars = []
    for bar in arrived_bi_list:
        bars.append(bar)
        hubs, bars = generate_hub(hubs, bars)

    # 写入数据库
    for hub in hubs:
        if not hub:
            continue
        start_dt = hub.elements[0].fx_a.dt.strftime('%Y-%m-%d %H:%M:%S')
        end_dt = hub.elements[-1].fx_b.dt.strftime('%Y-%m-%d %H:%M:%S')
        print(hub.elements)
        print(start_dt, end_dt)
        entry_id = -1 if not hub.entry else hub.entry.id
        leave_id = -1 if not hub.leave else hub.leave.id
        if hub.id >= 0:
            db.update('update cl_omphalos set end_datetime=\'%s\', gg=%s, dd=%s, leave_segment=%s where id=%s' % (
                end_dt, hub.GG, hub.DD, leave_id, hub.id))
        else:
            db.insert(
                'insert into cl_omphalos(stock_id, level, start_datetime, end_datetime, zd, zg, gg, dd, '
                'entry_segment, leave_segment, type) values(\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,%s,%s,%s,%s,\'%s\')' % (
                    symbol, freq, start_dt, end_dt, hub.ZG, hub.ZD, hub.GG, hub.DD, entry_id, leave_id,
                    component_type))


def cal_bs_point(symbol, freq):
    """计算趋势，并写入cl_trend数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    return None


def cal_bs_point(symbol, freq):
    """计算买卖点，并写入cl_point_result数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    return None


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    symbol = '600809.XSHG'
    # freq = '60m'
    freq = '1d'

    # 计算K线合并
    # cal_kx(symbol, freq)
    print('k线合并计算完成')

    # 计算分型
    cal_fx(symbol, freq)
    print('分型计算完成')

    # 计算笔
    cal_bi(symbol, freq)
    print('笔计算完成')

    # 计算中枢
    cal_zh(symbol, freq, 'bi')
    print('中枢计算完成')
