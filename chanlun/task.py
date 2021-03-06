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

from chanlun.analyze import remove_include, check_fxs, generate_bi, generate_biHub, remove_include_seq, \
    generate_line_by_bi, generate_line_by_seq, generate_lineHub
from chanlun.config.mysql_config import MysqlConfig
from chanlun.data.data_transfer import raw_bars_transfer, new_bars_transfer, fxs_transfer, bi_transfer, hub_transfer, \
    seq_transfer, line_transfer, bi2seq_transfer
from chanlun.objects import NewBar, Seq
from chanlun.utils.db_connector import MysqlUtils
import talib as tl

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
        macd = cal_macd_area(symbol, freq, bi.fx_a.dt, bi.fx_b.dt)
        state = 'done' if i < len(bi_list) - 2 else 'undone'
        if bi.id >= 0:
            if state == 'done':
                done_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                db.update(
                    'update cl_stroke set fx_a=%s , fx_b=%s , direction=\'%s\' , start_datetime=\'%s\' , '
                    'end_datetime=\'%s\' , high=%s , low=%s , state=\'%s\' , done_time=\'%s\', macd=%s where id=\'%s\'' % (
                        bi.fx_a.id, bi.fx_b.id, bi.direction, bi.fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'),
                        bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S'), bi.high, bi.low, state, done_time, macd, bi.id))
            else:
                db.update(
                    'update cl_stroke set fx_a=%s , fx_b=%s , direction=\'%s\' , start_datetime=\'%s\' , '
                    'end_datetime=\'%s\' , high=%s , low=%s , state=\'%s\', macd=%s where id=\'%s\'' % (
                        bi.fx_a.id, bi.fx_b.id, bi.direction, bi.fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'),
                        bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S'), bi.high, bi.low, state, macd, bi.id))
        else:
            if state == 'done':
                done_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                db.insert(
                    'insert into cl_stroke(stock_id, level, fx_a, fx_b, direction, start_datetime, end_datetime, high, '
                    'low, state, done_time, macd) values (\'%s\',\'%s\',%s, %s,\'%s\',\'%s\',\'%s\',%s,%s,\'%s\',\'%s\',%s)' % (
                        symbol, freq, bi.fx_a.id, bi.fx_b.id, bi.direction, bi.fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'),
                        bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S'), bi.high, bi.low, state, done_time, macd
                    ))
            else:
                db.insert(
                    'insert into cl_stroke(stock_id, level, fx_a, fx_b, direction, start_datetime, end_datetime, high, '
                    'low, state, macd) values (\'%s\',\'%s\',%s, %s,\'%s\',\'%s\',\'%s\',%s,%s,\'%s\',%s)' % (
                        symbol, freq, bi.fx_a.id, bi.fx_b.id, bi.direction, bi.fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'),
                        bi.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S'), bi.high, bi.low, state, macd
                    ))


def cal_seq(symbol, freq):
    """计算标准特征序列，并写入cl_sequence数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    # 最近向上标准特征序列
    raw_last_up_seq_list = db.get_all(
        'select * from cl_sequence where stock_id=\'%s\' and level=\'%s\' and direction=\'%s\' order by id desc limit 2' % (
            symbol, freq, 'up'))
    up_seq_list = seq_transfer(raw_last_up_seq_list, freq, symbol)
    # 最近向下标准特征序列
    raw_last_down_seq_list = db.get_all(
        'select * from cl_sequence where stock_id=\'%s\' and level=\'%s\' and direction=\'%s\' order by id desc limit 2' % (
            symbol, freq, 'down'))
    down_seq_list = seq_transfer(raw_last_down_seq_list, freq, symbol)

    # 读取未构成标准特征序列的所有笔
    init_len = len(up_seq_list) + len(down_seq_list)

    # 读取新进笔

    if init_len == 0:
        raw_bi_list = db.get_all(
            'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\'' % (symbol, freq))
    else:
        if len(up_seq_list) == 0:
            last_seq = down_seq_list[-1]
        elif len(down_seq_list) == 0:
            last_seq = up_seq_list[-1]
        else:
            last_seq = up_seq_list[-1] if up_seq_list[-1].dt > down_seq_list[-1] else down_seq_list[-1]

        raw_bi_list = db.get_all(
            'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and start_datetime>\'%s\'' % (
                symbol, freq, last_seq.dt))

    # 笔列表转化为未合并特征序列
    arrived_seq_list = bi2seq_transfer(raw_bi_list, freq, symbol)

    # 特征序列进行包含处理
    for seq in arrived_seq_list:
        seq_list = up_seq_list if seq.direction == 'up' else down_seq_list
        if len(seq_list) < 2:
            seq_list.append(Seq(symbol=seq.symbol, id=seq.id, freq=seq.freq, start_dt=seq.start_dt, end_dt=seq.end_dt,
                                high=seq.high, low=seq.low, direction=seq.direction))
        else:
            k1, k2 = seq_list[-2:]
            # 合并处理
            has_include, k3 = remove_include_seq(k1, k2, seq)
            if has_include:
                seq_list[-1] = k3
            else:
                seq_list.append(k3)

    # 合并向上特征序列和向下特征序列
    pos1 = 0
    pos2 = 0
    seq_list_res = []
    while pos1 < len(up_seq_list) or pos2 < len(down_seq_list):
        if pos2 == len(down_seq_list) or (
                pos1 < len(up_seq_list) and up_seq_list[pos1].start_dt < down_seq_list[pos2].start_dt):
            seq_list_res.append(up_seq_list[pos1])
            pos1 = pos1 + 1
        else:
            seq_list_res.append(down_seq_list[pos2])
            pos2 = pos2 + 1

    seq_list_res = arrived_seq_list
    # 标准特征序列写入数据库
    for i in range(len(seq_list_res)):
        seq = seq_list_res[i]
        if seq.id > -1:
            db.update(
                'update cl_sequence set start_datetime=\'%s\' and end_datetime=\'%s\' and high=%s and low=%s where id=%s' % (
                    seq.start_dt.strftime('%Y-%m-%d %H:%M:%S'), seq.end_dt.strftime('%Y-%m-%d %H:%M:%S'), seq.high,
                    seq.low, seq.id))
        else:
            db.insert(
                'insert into cl_sequence(stock_id, level, start_datetime, end_datetime, high, low, direction) values(\'%s\',\'%s\', \'%s\','
                '\'%s\',%s,%s,\'%s\')' % (
                    symbol, freq, seq.start_dt.strftime('%Y-%m-%d %H:%M:%S'), seq.end_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    seq.high, seq.low, seq.direction))


def cal_xd_by_seq(symbol, freq):
    """计算线段，并写入cl_paragraph数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    raw_last_line_list = db.get_all(
        'select * from cl_paragraph where stock_id=\'%s\' and level=\'%s\' order by id desc limit 2' % (symbol, freq))
    line_list = line_transfer(raw_last_line_list, freq, symbol, True)
    line_list.reverse()

    for line in line_list:
        db.delete('delete from cl_paragraph where id=%s' % line.id)

    init_len = len(line_list)

    if init_len == 0:
        raw_arrived_seqs = db.get_all(
            'select * from cl_sequence where stock_id=\'%s\' and level=\'%s\'' % (symbol, freq))
    else:
        raw_arrived_seqs = db.get_all('select * from cl_sequence where stock_id=\'%s\' and level=\'%s\' and '
                                      'start_datetime>=\'%s\'' % (
                                          symbol, freq, line_list[-1].fx_a.elements[0].start_dt))
    arrived_seqs = seq_transfer(raw_arrived_seqs, freq, symbol)

    # 生成线段
    seqs = []
    for seq in arrived_seqs:
        seqs.append(seq)
        line_list, seqs = generate_line_by_seq(line_list, seqs)

    for i in range(len(line_list)):
        line = line_list[i]

        elements_str = line.seqs[0].start_dt.strftime('%Y-%m-%d %H:%M:%S') + ',' + line.seqs[-1].end_dt.strftime(
            '%Y-%m-%d %H:%M:%S')
        fx_a_ids = str(line.fx_a.elements[0].id) + ',' + str(line.fx_a.elements[1].id) + ',' + str(
            line.fx_a.elements[2].id)
        fx_b_ids = str(line.fx_b.elements[0].id) + ',' + str(line.fx_b.elements[1].id) + ',' + str(
            line.fx_b.elements[2].id)
        db.insert(
            'insert into cl_paragraph(stock_id, level, direction, start_datetime, end_datetime, high, low, elements, '
            'fx_a_ids, fx_b_ids) values(\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,\'%s\',\'%s\',\'%s\')' % (
                symbol, freq, line.direction, line.start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                line.end_dt.strftime('%Y-%m-%d %H:%M:%S'), line.high, line.low, elements_str, fx_a_ids, fx_b_ids))


def cal_zh(symbol, freq, component_type):
    """计算中枢，并写入cl_omphalos数据库表

        :param symbol: 股票代码
        :param freq: K线级别
        :param component_type: 构件（bi/xd）
    """
    # 读取最近的中枢
    global points
    raw_last_hub = db.get_one(
        'select * from cl_omphalos where stock_id=\'%s\' and level=\'%s\' and type=\'%s\' order by id desc limit 1' % (
            symbol, freq, component_type))
    hubs = hub_transfer(raw_last_hub, freq, symbol, component_type)

    if component_type == 'bi':
        # 获取最近中枢之后的笔
        if len(hubs) == 0:
            raw_arrived_bi_list = db.get_all(
                'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\'' % (symbol, freq))
        else:
            raw_arrived_bi_list = db.get_all(
                'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and start_datetime>=\'%s\'' % (
                    symbol, freq, hubs[-1].elements[-1].fx_b.dt))

        arrived_bi_list = bi_transfer(raw_arrived_bi_list, freq, symbol)

        # 生成中枢
        bars = []
        points = []
        for bar in arrived_bi_list:
            bars.append(bar)
            hubs, bars, points = generate_biHub(hubs, bars, points)
    elif component_type == 'xd':
        # 获取最近中枢之后的笔
        if len(hubs) == 0:
            raw_arrived_line_list = db.get_all(
                'select * from cl_paragraph where stock_id=\'%s\' and level=\'%s\'' % (symbol, freq))
        else:
            raw_arrived_line_list = db.get_all(
                'select * from cl_paragraph where stock_id=\'%s\' and level=\'%s\' and start_datetime>=\'%s\'' % (
                    symbol, freq, hubs[-1].elements[-1].fx_b.dt))
        arrived_line_list = line_transfer(raw_arrived_line_list, freq, symbol, False)

        bars = []
        points = []
        for bar in arrived_line_list:
            bars.append(bar)
            hubs, bars, points = generate_lineHub(hubs, bars, points)

    # 中枢写入数据库
    for hub in hubs:
        if not hub:
            continue
        if component_type == 'bi':
            start_dt = hub.elements[0].fx_a.dt.strftime('%Y-%m-%d %H:%M:%S')
            end_dt = hub.elements[-1].fx_b.dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            start_dt = hub.elements[0].start_dt
            end_dt = hub.elements[-1].end_dt
        entry_id = -1 if not hub.entry else hub.entry.id
        leave_id = -1 if not hub.leave else hub.leave.id
        if hub.id >= 0:
            db.update('update cl_omphalos set end_datetime=\'%s\', gg=%s, dd=%s, leave_segment=%s where id=%s' % (
                end_dt, hub.GG, hub.DD, leave_id, hub.id))
        else:
            db.insert(
                'insert into cl_omphalos(stock_id, level, start_datetime, end_datetime, zd, zg, gg, dd, '
                'entry_segment, leave_segment, type) values(\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,%s,%s,%s,%s,\'%s\')' % (
                    symbol, freq, start_dt, end_dt, hub.ZD, hub.ZG, hub.GG, hub.DD, entry_id, leave_id,
                    component_type))
    # 三类买卖点写入数据库
    for point in points:
        db.insert(
            'insert into cl_point_result(stock_id, level, point, type, high, low, component_type) values(\'%s\',\'%s\',\'%s\',\'%s\','
            '%s,%s,\'%s\')' % (symbol, freq, point.dt.strftime('%Y-%m-%d %H:%M:%S'), point.type, point.high, point.low, component_type))


def cl_trend(symbol, freq):
    """计算趋势，并写入cl_trend数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    # 获取最新的中枢
    hubs = db.get_all(
        'select * from cl_omphalos where stock_id=\'%s\' and level=\'%s\' order by id' % (symbol, freq))

    return None


def rebuild_bs_point(symbol, freq):
    """计算买卖点，并写入cl_point_result数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    # 买卖点强依赖中枢
    hubs = db.get_all(
        'select * from cl_omphalos where stock_id=\'%s\' and level=\'%s\' order by id' % (symbol, freq))
    if not hubs:
        return None
    for i in range(1, len(hubs) - 1):
        if hubs[i][6] > hubs[i - 1][5] and hubs[i][6] > hubs[i + 1][5]:
            raw_arrived_bi_list = db.get_all(
                'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and start_datetime>=\'%s\' and start_datetime<=\'%s\'' % (
                    symbol, freq, hubs[i][3], hubs[i + 1][3]))
            up_pos = None
            up_dt = None
            for bi in raw_arrived_bi_list:
                if bi[5] == 'up':
                    if not up_pos:
                        up_pos = bi[9]
                        up_dt = bi[7]
                    else:
                        if bi[9] > up_pos:
                            up_pos = bi[9]
                            up_dt = bi[7]
            db.insert(
                "insert into cl_point_result(stock_id, level, point, type, evaluation_time, valid, high, low) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    symbol, freq, up_dt, 'S1', hubs[i + 1][4], 1, up_pos, 0))
            print(up_dt)
            print(up_pos)
            print("=========S1=========")
            up_pos2 = None
            up_dt2 = None
            i = -1
            for bi in raw_arrived_bi_list:
                if i >= 0:
                    i += 1
                if bi[7] == up_dt:
                    i = 0
                if i == 2:
                    up_pos2 = bi[9]
                    up_dt2 = bi[7]
                    break
            print(up_dt2)
            print(up_pos2)
            print("=========S2=========")
            db.insert(
                "insert into cl_point_result(stock_id, level, point, type, evaluation_time, valid, high, low) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    symbol, freq, up_dt2, 'S2', hubs[i + 1][4], 1, up_pos2, 0))
        if hubs[i][5] < hubs[i - 1][6] and hubs[i][5] < hubs[i + 1][6]:
            raw_arrived_bi_list = db.get_all(
                'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and start_datetime>=\'%s\' and start_datetime<=\'%s\'' % (
                    symbol, freq, hubs[i][3], hubs[i + 1][3]))
            up_pos = None
            up_dt = None
            for bi in raw_arrived_bi_list:
                if bi[5] == 'down':
                    if not up_pos:
                        up_pos = bi[10]
                        up_dt = bi[7]
                    else:
                        if bi[10] < up_pos:
                            up_pos = bi[10]
                            up_dt = bi[7]
            # db.insert(
            #     'insert into cl_point(stock_id, level, point, type, evaluation_time, valid, high, low) values(\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,%s,%s,%s,%s,\'%s\')' % (
            #         symbol, freq, up_dt, 'B1', hubs[2][4], 1, up_pos, 0))
            print(up_dt)
            print(up_pos)
            print("=========B1=========")
            db.insert(
                "insert into cl_point_result(stock_id, level, point, type, evaluation_time, valid, high, low) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    symbol, freq, up_dt, 'B1', hubs[i + 1][4], 1, 0, up_pos))
            up_pos2 = None
            up_dt2 = None
            i = -1
            for bi in raw_arrived_bi_list:
                if i >= 0:
                    i += 1
                if bi[7] == up_dt:
                    i = 0
                if i == 2:
                    up_pos2 = bi[10]
                    up_dt2 = bi[7]
                    break
            print(up_dt2)
            print(up_pos2)
            print("=========B2=========")
            db.insert(
                "insert into cl_point_result(stock_id, level, point, type, evaluation_time, valid, high, low) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    symbol, freq, up_dt2, 'B2', hubs[i + 1][4], 1, 0, up_pos2))


def cal_bs_point(symbol, freq):
    """计算买卖点，并写入cl_point_result数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    # 买卖点强依赖中枢
    hubs = db.get_all(
        'select * from cl_omphalos where stock_id=\'%s\' and level=\'%s\' order by id desc limit 3' % (symbol, freq))
    hubs = list(hubs)
    hubs.reverse()
    if len(hubs) >= 3:
        if hubs[1][6] > hubs[0][5] and hubs[1][6] > hubs[2][5]:
            raw_arrived_bi_list = db.get_all(
                'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and start_datetime>=\'%s\' and start_datetime<=\'%s\'' % (
                    symbol, freq, hubs[1][3], hubs[2][3]))
            up_pos = None
            up_dt = None
            for bi in raw_arrived_bi_list:
                if bi[5] == 'up':
                    if not up_pos:
                        up_pos = bi[9]
                        up_dt = bi[7]
                    else:
                        if bi[9] > up_pos:
                            up_pos = bi[9]
                            up_dt = bi[7]
            db.insert(
                "insert into cl_point_result(stock_id, level, point, type, evaluation_time, valid, high, low) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    symbol, freq, up_dt, 'S1', hubs[2][4], 1, up_pos, 0))
            print(up_dt)
            print(up_pos)
            print("=========S1=========")
            up_pos2 = None
            up_dt2 = None
            i = -1
            for bi in raw_arrived_bi_list:
                if i >= 0:
                    i += 1
                if bi[7] == up_dt:
                    i = 0
                if i == 2:
                    up_pos2 = bi[9]
                    up_dt2 = bi[7]
                    break
            print(up_dt2)
            print(up_pos2)
            print("=========S2=========")
            db.insert(
                "insert into cl_point_result(stock_id, level, point, type, evaluation_time, valid, high, low) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    symbol, freq, up_dt2, 'S2', hubs[2][4], 1, up_pos2, 0))

        if hubs[1][5] < hubs[0][6] and hubs[1][5] < hubs[2][6]:
            raw_arrived_bi_list = db.get_all(
                'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and start_datetime>=\'%s\' and start_datetime<=\'%s\'' % (
                    symbol, freq, hubs[1][3], hubs[2][3]))
            up_pos = None
            up_dt = None
            for bi in raw_arrived_bi_list:
                if bi[5] == 'down':
                    if not up_pos:
                        up_pos = bi[10]
                        up_dt = bi[7]
                    else:
                        if bi[10] > up_pos:
                            up_pos = bi[10]
                            up_dt = bi[7]
            print(up_dt)
            print(up_pos)
            print("=========B1=========")
            db.insert(
                "insert into cl_point_result(stock_id, level, point, type, evaluation_time, valid, high, low) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    symbol, freq, up_dt, 'B1', hubs[2][4], 1, 0, up_pos))
            up_pos2 = None
            up_dt2 = None
            i = -1
            for bi in raw_arrived_bi_list:
                if i >= 0:
                    i += 1
                if bi[7] == up_dt:
                    i = 0
                if i == 2:
                    up_pos2 = bi[10]
                    up_dt2 = bi[7]
                    break
            print(up_dt2)
            print(up_pos2)
            print("=========B2=========")
            db.insert(
                "insert into cl_point_result(stock_id, level, point, type, evaluation_time, valid, high, low) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    symbol, freq, up_dt2, 'B2', hubs[2][4], 1, 0, up_pos2))


def cal_xd_by_bi(symbol, freq):
    """根据笔计算线段，并写入cl_paragraph数据库表

        :param symbol: 股票代码
        :param freq: K线级别
    """
    raw_last_line_list = db.get_all(
        'select * from cl_paragraph where stock_id=\'%s\' and level=\'%s\' order by id desc limit 2' % (symbol, freq))
    line_list = line_transfer(raw_last_line_list, freq, symbol, False)
    line_list.reverse()

    for line in line_list:
        db.delete('delete from cl_paragraph where id=%s' % line.id)

    init_len = len(line_list)

    if init_len == 0:
        raw_arrived_bi_list = db.get_all(
            'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\'' % (symbol, freq))
    else:
        next_bi = db.get_one('select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and '
                             'start_datetime=\'%s\'' % (symbol, freq, line_list[-1].end_dt))
        if next_bi is None:
            line_list.pop(-1)
        if len(line_list) == 0:
            raw_arrived_bi_list = db.get_all(
                'select * from cl_stroke where stock_id=\'%s\' and level=\'%s\'' % (symbol, freq))
        else:
            raw_arrived_bi_list = db.get_all('select * from cl_stroke where stock_id=\'%s\' and level=\'%s\' and '
                                             'start_datetime>=\'%s\'' % (symbol, freq, line_list[-1].end_dt))
    arrived_bis = bi_transfer(raw_arrived_bi_list, freq, symbol)

    # 生成线段
    bi_list = []
    for bi in arrived_bis:
        bi_list.append(bi)
        line_list, bi_list = generate_line_by_bi(line_list, bi_list)

    for i in range(len(line_list)):
        line = line_list[i]

        elements_str = (line.seqs[0].start_dt.strftime('%Y-%m-%d %H:%M:%S') + ',' + line.seqs[-1].end_dt.strftime(
            '%Y-%m-%d %H:%M:%S')) if line.seqs is not None and len(line.seqs) > 1 else ""
        fx_a_ids = (str(line.fx_a.elements[0].id) + ',' + str(line.fx_a.elements[1].id) + ',' + str(
            line.fx_a.elements[2].id)) if line.fx_a is not None else ""
        fx_b_ids = str(line.fx_b.elements[0].id) + ',' + str(line.fx_b.elements[1].id) + ',' + str(
            line.fx_b.elements[2].id) if line.fx_b is not None else ""
        macd = cal_macd_area(symbol, freq, line.start_dt, line.end_dt)
        db.insert(
            'insert into cl_paragraph(stock_id, level, direction, start_datetime, end_datetime, high, low, elements, '
            'fx_a_ids, fx_b_ids, macd) values(\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,\'%s\',\'%s\',\'%s\',\'%s\')' % (
                symbol, freq, line.direction, line.start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                line.end_dt.strftime('%Y-%m-%d %H:%M:%S'), line.high, line.low, elements_str, fx_a_ids, fx_b_ids, macd))

def cal_macd_area(symbol, freq, start_dt, end_dt):
    table_name = symbol.replace(".", "_") + "_" + freq
    k_left_list = db.get_all('select * from %s where Date<\'%s\' order by Date desc limit 40' % (table_name, start_dt))
    left_bars = raw_bars_transfer(k_left_list, symbol, freq)
    left_bars.reverse()
    k_right_list = db.get_all('select * from %s where Date>=\'%s\' and Date <= \'%s\'' % (table_name, start_dt, end_dt))
    right_bars = raw_bars_transfer(k_right_list, symbol, freq)
    k_num = len(right_bars)

    bars = left_bars + right_bars


    # 计算macd
    dif, dea, macd = tl.MACD(np.array([bar.close for bar in bars]), fastperiod=12, slowperiod=26, signalperiod=9)
    macd = macd[len(macd)-k_num:]
    sum = 0
    for item in macd:
        if np.isnan(item):
            item = 0
        sum += abs(item)

    return sum
if __name__ == '__main__':
    freqs = ['1d', '60m', '30m', '5m', '1m']
    symbol = '600809.XSHG'
    # freq = '60m'
    # freq = '1d'
    # freq = '30m'
    # freq = '5m'
    # freq = '1m'

    # 计算K线合并
    # cal_kx(symbol, freq)
    # print('k线合并计算完成')

    # 计算分型
    # cal_fx(symbol, freq)
    # print('分型计算完成')

    # 计算笔
    # cal_bi(symbol, freq)
    # print('笔计算完成')

    # 批量计算笔
    # for freq in freqs:
    #     cal_bi(symbol, freq)
    #     print(freq+'笔计算完成')

    # 计算笔中枢
    # cal_zh(symbol, freq, 'bi')
    # print('笔中枢计算完成')



    # 计算标准特征序列
    # cal_seq(symbol, freq)
    # print('标准特征序列计算完成')

    # 根据特征序列计算线段
    # cal_xd_by_seq(symbol, freq)

    # 根据笔计算线段
    # cal_xd_by_bi(symbol, freq)
    # print('线段计算完成')

    # 批量计算线段
    # for freq in freqs:
    #     cal_xd_by_bi(symbol, freq)
    #     print(freq+'线段计算完成')

    # 计算线段中枢
    # cal_zh(symbol, freq, 'xd')
    # print('线段中枢计算完成')

    # 计算趋势
    # cl_trend(symbol, freq)
    # print('趋势计算完成')

    # 计算买卖点
    # cal_bs_point(symbol, freq)
    # print('买卖点计算完成')

    # 测试买卖点
    # rebuild_bs_point(symbol, freq)
