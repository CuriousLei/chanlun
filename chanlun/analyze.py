# coding: utf-8
import os
import webbrowser
from typing import List, Callable
from datetime import datetime
import pandas as pd
import traceback
from collections import OrderedDict
from pyecharts.charts import Tab
from pyecharts.components import Table
from pyecharts.options import ComponentTitleOpts

from chanlun.config.mysql_config import MysqlConfig
from chanlun.utils.kline_generator import KlineGenerator
from chanlun.enums import Mark, Direction, Operate
from chanlun.objects import BI, FakeBI, FX, RawBar, NewBar, Event, Hub, Line, Seq, SeqFX, Point
from chanlun.utils.echarts_plot import kline_pro
from chanlun.utils.db_connector import MysqlUtils

db = MysqlUtils(MysqlConfig.host,
                MysqlConfig.port,
                MysqlConfig.user,
                MysqlConfig.password,
                MysqlConfig.dbname,
                MysqlConfig.coding)


def create_fake_bis(fxs: List[FX]) -> List[FakeBI]:
    """创建 fake_bis 列表

    :param fxs: 分型序列，必须顶底分型交替
    :return: fake_bis
    """
    if len(fxs) % 2 != 0:
        fxs = fxs[:-1]

    fake_bis = []
    for i in range(1, len(fxs)):
        fx1 = fxs[i - 1]
        fx2 = fxs[i]
        assert fx1.mark != fx2.mark
        if fx1.mark == Mark.D:
            fake_bi = FakeBI(symbol=fx1.symbol, sdt=fx1.dt, edt=fx2.dt, direction=Direction.Up,
                             high=fx2.high, low=fx1.low, power=round(fx2.high - fx1.low, 2))
        elif fx1.mark == Mark.G:
            fake_bi = FakeBI(symbol=fx1.symbol, sdt=fx1.dt, edt=fx2.dt, direction=Direction.Down,
                             high=fx1.high, low=fx2.low, power=round(fx1.high - fx2.low, 2))
        else:
            raise ValueError
        fake_bis.append(fake_bi)
    return fake_bis


def remove_include(k1: NewBar, k2: NewBar, k3: RawBar):
    """去除包含关系：输入三根k线，其中k1和k2为没有包含关系的K线，k3为原始K线"""
    if k1.high < k2.high:
        direction = Direction.Up
    elif k1.high > k2.high:
        direction = Direction.Down
    else:
        k4 = NewBar(symbol=k3.symbol, id=k3.id, freq=k3.freq, dt=k3.dt, open=k3.open,
                    close=k3.close, high=k3.high, low=k3.low, vol=k3.vol, elements=[k3])
        return False, k4

    # 判断 k2 和 k3 之间是否存在包含关系，有则处理
    if (k2.high <= k3.high and k2.low >= k3.low) or (k2.high >= k3.high and k2.low <= k3.low):
        if direction == Direction.Up:
            high = max(k2.high, k3.high)
            low = max(k2.low, k3.low)
            dt = k2.dt if k2.high > k3.high else k3.dt
        elif direction == Direction.Down:
            high = min(k2.high, k3.high)
            low = min(k2.low, k3.low)
            dt = k2.dt if k2.low < k3.low else k3.dt
        else:
            raise ValueError

        if k3.open > k3.close:
            open_ = high
            close = low
        else:
            open_ = low
            close = high
        vol = None if k2.vol is None or k3.vol is None else k2.vol + k3.vol
        # 这里有一个隐藏Bug，len(k2.elements) 在一些及其特殊的场景下会有超大的数量，具体问题还没找到；
        # 临时解决方案是直接限定len(k2.elements)<=100
        elements = [x for x in k2.elements[:100] if x.dt != k3.dt] + [k3]
        k4 = NewBar(symbol=k3.symbol, id=k2.id, freq=k2.freq, dt=dt, open=open_,
                    close=close, high=high, low=low, vol=vol, elements=elements)
        return True, k4
    else:
        k4 = NewBar(symbol=k3.symbol, id=k3.id, freq=k3.freq, dt=k3.dt, open=k3.open,
                    close=k3.close, high=k3.high, low=k3.low, vol=k3.vol, elements=[k3])
        return False, k4


def remove_include_seq(k1: Seq, k2: Seq, k3: Seq):
    """去除包含关系：输入三根特征序列元素，其中k1和k2为没有包含关系的元素，k3为原始元素"""
    if k1.high < k2.high:
        direction = Direction.Up
    elif k1.high > k2.high:
        direction = Direction.Down
    else:
        k4 = Seq(symbol=k3.symbol, id=k3.id, freq=k3.freq, high=k3.high, low=k3.low, start_dt=k3.start_dt,
                 end_dt=k3.end_dt, direction=k3.direction)
        return False, k4

    # 判断 k2 和 k3 之间是否存在包含关系，有则处理
    if (k2.high <= k3.high and k2.low >= k3.low) or (k2.high >= k3.high and k2.low <= k3.low):
        if direction == Direction.Up:
            high = max(k2.high, k3.high)
            low = max(k2.low, k3.low)
            start_dt = k2.start_dt if k2.high > k3.high else k3.start_dt
            end_dt = k2.end_dt if k2.high > k3.high else k3.end_dt
        elif direction == Direction.Down:
            high = min(k2.high, k3.high)
            low = min(k2.low, k3.low)
            start_dt = k2.start_dt if k2.low < k3.low else k3.start_dt
            end_dt = k2.end_dt if k2.low < k3.low else k3.end_dt
        else:
            raise ValueError

        k4 = Seq(symbol=k3.symbol, id=k2.id, freq=k2.freq, start_dt=start_dt, end_dt=end_dt,
                 high=high, low=low, direction=k2.direction)
        return True, k4
    else:
        k4 = Seq(symbol=k3.symbol, id=k3.id, freq=k3.freq, start_dt=k3.start_dt, end_dt=k3.end_dt,
                 high=k3.high, low=k3.low, direction=k3.direction)
        return False, k4


def check_fx(k1: NewBar, k2: NewBar, k3: NewBar):
    """查找分型"""
    fx = None
    fx_id = -1
    if k1.high < k2.high > k3.high and k1.low < k2.low > k3.low:
        power = "强" if k3.close < k1.low else "弱"

        fx = FX(symbol=k1.symbol, freq=k1.freq, dt=k2.dt, mark='G', high=k2.high, low=k2.low,
                fx=k2.high, elements=[k1, k2, k3], power=power, id=fx_id)

    if k1.low > k2.low < k3.low and k1.high > k2.high < k3.high:
        power = "强" if k3.close > k1.high else "弱"
        fx = FX(symbol=k1.symbol, freq=k1.freq, dt=k2.dt, mark='D', high=k2.high, low=k2.low,
                fx=k2.low, elements=[k1, k2, k3], power=power, id=fx_id)

    return fx


def check_fxs(bars: List[NewBar]) -> List[FX]:
    """输入一串无包含关系K线，查找其中所有分型"""
    fxs = []
    for i in range(1, len(bars) - 1):
        fx: FX = check_fx(bars[i - 1], bars[i], bars[i + 1])
        if isinstance(fx, FX):
            # 这里可能隐含Bug，默认情况下，fxs本身是顶底交替的，但是对于一些特殊情况下不是这样，这是不对的。
            # 临时处理方案，强制要求fxs序列顶底交替
            if len(fxs) >= 2 and fx.mark == fxs[-1].mark:
                fxs.pop()
            fxs.append(fx)

    return fxs


def check_fx_seq(k1: Seq, k2: Seq, k3: Seq):
    """查找特征序列分型"""
    fx = None
    fx_id = -1
    if k1.high < k2.high > k3.high and k1.low < k2.low > k3.low:
        # power = "强" if k3.close < k1.low else "弱"
        fx = SeqFX(symbol=k1.symbol, freq=k1.freq, dt=k2.start_dt, mark='G', high=k2.high, low=k2.low,
                   fx=k2.high, elements=[k1, k2, k3], power='', id=fx_id, direction=k1.direction)

    if k1.low > k2.low < k3.low and k1.high > k2.high < k3.high:
        # power = "强" if k3.close > k1.high else "弱"
        fx = SeqFX(symbol=k1.symbol, freq=k1.freq, dt=k2.start_dt, mark='D', high=k2.high, low=k2.low,
                   fx=k2.low, elements=[k1, k2, k3], power='', id=fx_id, direction=k1.direction)

    return fx


def check_fxs_seq(bars: List[Seq]) -> List[SeqFX]:
    """输入一串无包含关系特征序列，查找其中所有分型"""
    fxs = []
    for i in range(1, len(bars) - 1):
        fx: SeqFX = check_fx_seq(bars[i - 1], bars[i], bars[i + 1])
        if isinstance(fx, SeqFX):
            # 这里可能隐含Bug，默认情况下，fxs本身是顶底交替的，但是对于一些特殊情况下不是这样，这是不对的。
            # 临时处理方案，强制要求fxs序列顶底交替
            if len(fxs) >= 2 and fx.mark == fxs[-1].mark:
                fxs.pop()
            fxs.append(fx)

    return fxs


def get_next_fx(fxs: List[FX], idx):
    fx_a = fxs[idx]
    fx_b = None
    for i in range(idx, len(fxs)):
        x = fxs[i]
        if fx_a.mark == 'D':
            if x.mark == 'G' and x.dt > fx_a.dt and x.fx > fx_a.fx:
                fx_b = x
                break
        elif fx_a.mark == 'G':
            if x.mark == 'D' and x.dt > fx_a.dt and x.fx < fx_a.fx:
                fx_b = x
                break
        else:
            raise ValueError
    return fx_b


# def check_bi(fxs: List[FX]):
#     """输入一串分型，查找其中的一笔
#
#         :param fxs: 分型列表
#         :return:最左一笔和后续分型列表
#     """
#     fxs_ = fxs
#     if len(fxs) < 2:
#         return None, fxs_
#
#     fx_a = fxs[0]
#     try:
#         if fxs[0].mark == Mark.D:
#             direction = 'up'
#             fxs_b = [x for x in fxs if x.mark == Mark.G and x.dt > fx_a.dt and x.fx > fx_a.fx]
#             if not fxs_b:
#                 return None, fxs_
#             fx_b = fxs_b[0]
#             for fx in fxs_b:
#                 if fx.high >= fx_b.high:
#                     fx_b = fx
#         elif fxs[0].mark == Mark.G:
#             direction = 'down'
#             fxs_b = [x for x in fxs if x.mark == Mark.D and x.dt > fx_a.dt and x.fx < fx_a.fx]
#             if not fxs_b:
#                 return None, fxs_
#             fx_b = fxs_b[0]
#             for fx in fxs_b[1:]:
#                 if fx.low <= fx_b.low:
#                     fx_b = fx
#         else:
#             raise ValueError
#     except:
#         traceback.print_exc()
#         return None, fxs_
#
#     db_bars = db.get_one(
#         'select * from cl_kx where id>=%s and id<=%s ' % (fx_a.elements[0].id, fx_b.elements[2].id))
#     bars = new_bars_transfer(db_bars, fx_a.elements[0].freq, fx_a.symbol)
#
#     # 判断fx_a和fx_b价格区间是否存在包含关系
#     ab_include = (fx_a.high > fx_b.high and fx_a.low < fx_b.low) or (fx_a.high < fx_b.high and fx_a.low > fx_b.low)
#
#     if len(bars) >= 7 and not ab_include:
#         # 计算笔的相关属性
#         power_price = round(abs(fx_b.fx - fx_a.fx), 2)
#         fxs_inner = [x for x in fxs if fx_a.elements[0].dt <= x.dt <= fx_b.elements[2].dt]
#         fxs_ = [x for x in fxs if x.dt >= fx_b.elements[0].dt]
#
#         bi = BI(symbol=fx_a.symbol, fx_a=fx_a, fx_b=fx_b, fxs=fxs_inner, bars=bars,
#                 direction=direction, power=power_price, high=max(fx_a.high, fx_b.high),
#                 low=min(fx_a.low, fx_b.low), id=0)
#
#         return bi, fxs_
#     else:
#         return None, fxs_


def generate_bi(bi_list: List[BI], bars_ubi: List[NewBar]):
    """根据K线列表生成笔，每次生成一根笔

    :param bi_list: 笔列表
    :param bars_ubi: 已合并的k线列表
    :return: bi_list, bars_ubi: 更新后的笔列表和k线列表
    """
    if len(bars_ubi) < 3:
        return bi_list, bars_ubi

    # 查找笔
    if not bi_list or len(bi_list) < 1:
        # 第一个笔的查找
        fxs = check_fxs(bars_ubi)
        if not fxs:
            return bi_list, bars_ubi

        fx_a = fxs[0]
        fxs_a = [x for x in fxs if x.mark == fx_a.mark]
        for fx in fxs_a:
            if (fx_a.mark == 'D' and fx.low <= fx_a.low) \
                    or (fx_a.mark == 'G' and fx.high >= fx_a.high):
                fx_a = fx
        bars_ubi = [x for x in bars_ubi if x.dt >= fx_a.elements[0].dt]

        bi, bars_ubi_ = check_bi(bars_ubi)
        if isinstance(bi, BI):
            bi_list.append(bi)
        bars_ubi = bars_ubi_
        return bi_list, bars_ubi

    last_bi = bi_list[-1]

    # 如果上一笔被破坏，将上一笔的bars与bars_ubi进行合并
    min_low_ubi = min([x.low for x in bars_ubi[2:]])
    max_high_ubi = max([x.high for x in bars_ubi[2:]])

    if last_bi.direction == 'up' and max_high_ubi > last_bi.high:
        if min_low_ubi < last_bi.low and len(bi_list) > 2:
            bars_ubi_a = bi_list[-2].bars \
                         + [x for x in bi_list[-1].bars if x.dt > bi_list[-2].bars[-1].dt] \
                         + [x for x in bars_ubi if x.dt > bi_list[-1].bars[-1].dt]
            bi_list.pop(-1)
            bi_list.pop(-1)
        else:
            bars_ubi_a = last_bi.bars + [x for x in bars_ubi if x.dt > last_bi.bars[-1].dt]
            bi_list.pop(-1)
    elif last_bi.direction == 'down' and min_low_ubi < last_bi.low:
        if max_high_ubi > last_bi.high and len(bi_list) > 2:
            bars_ubi_a = bi_list[-2].bars \
                         + [x for x in bi_list[-1].bars if x.dt > bi_list[-2].bars[-1].dt] \
                         + [x for x in bars_ubi if x.dt > bi_list[-1].bars[-1].dt]
            bi_list.pop(-1)
            bi_list.pop(-1)
        else:
            bars_ubi_a = last_bi.bars + [x for x in bars_ubi if x.dt > last_bi.bars[-1].dt]
            bi_list.pop(-1)
    else:
        bars_ubi_a = bars_ubi

    if len(bars_ubi_a) > 300:
        print("{} - {} 未完成笔延伸超长，延伸数量: {}".format(last_bi.symbol, last_bi.fx_a.elements[0].freq, len(bars_ubi_a)))
    bi, bars_ubi_ = check_bi(bars_ubi_a)
    bars_ubi = bars_ubi_
    if isinstance(bi, BI):
        bi_list.append(bi)

    return bi_list, bars_ubi


def check_bi(bars: List[NewBar]):
    """输入一串无包含关系K线，查找其中的一笔"""
    fxs = check_fxs(bars)
    if len(fxs) < 2:
        return None, bars
    fx_a = fxs[0]
    try:
        if fxs[0].mark == 'D':
            direction = 'up'
            fxs_b = [x for x in fxs if x.mark == 'G' and x.dt > fx_a.dt and x.fx > fx_a.fx]
            if not fxs_b:
                return None, bars
            fx_b = fxs_b[0]
            for fx in fxs_b:
                if fx.high >= fx_b.high:
                    fx_b = fx
        elif fxs[0].mark == 'G':
            direction = 'down'
            fxs_b = [x for x in fxs if x.mark == 'D' and x.dt > fx_a.dt and x.fx < fx_a.fx]
            if not fxs_b:
                return None, bars
            fx_b = fxs_b[0]
            for fx in fxs_b[1:]:
                if fx.low <= fx_b.low:
                    fx_b = fx
        else:
            raise ValueError
    except:
        traceback.print_exc()
        return None, bars

    bars_a = [x for x in bars if fx_a.elements[0].dt <= x.dt <= fx_b.elements[2].dt]
    bars_b = [x for x in bars if x.dt >= fx_b.elements[0].dt]

    # 判断fx_a和fx_b价格区间是否存在包含关系
    ab_include = (fx_a.high > fx_b.high and fx_a.low < fx_b.low) or (fx_a.high < fx_b.high and fx_a.low > fx_b.low)

    if len(bars_a) >= 7 and not ab_include:
        # 计算笔的相关属性
        power_price = round(abs(fx_b.fx - fx_a.fx), 2)
        fxs_ = [x for x in fxs if fx_a.elements[0].dt <= x.dt <= fx_b.elements[2].dt]

        # 为fx_a和fx_b填充id
        fx_a_id_res = db.get_one(
            'select id from cl_shape where datetime=\'%s\' and stock_id=\'%s\' and level=\'%s\'' % (
                fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'), fx_a.symbol, fx_a.elements[0].freq))
        fx_b_id_res = db.get_one(
            'select id from cl_shape where datetime=\'%s\' and stock_id=\'%s\' and level=\'%s\'' % (
                fx_b.dt.strftime('%Y-%m-%d %H:%M:%S'), fx_b.symbol, fx_b.elements[0].freq))
        if fx_a_id_res and fx_b_id_res:
            fx_a.id = fx_a_id_res[0]
            fx_b.id = fx_b_id_res[0]
        else:
            raise ValueError
        # 生成BI对象
        bi = BI(symbol=fx_a.symbol, freq=fx_a.freq, fx_a=fx_a, fx_b=fx_b, fxs=fxs_,
                direction=direction, power=power_price, high=max(fx_a.high, fx_b.high),
                low=min(fx_a.low, fx_b.low), bars=bars_a, id=-1)

        return bi, bars_b
    else:
        return None, bars


def generate_line(line_list: List[Line], seqs: List[Seq]):
    """根据(标准特征序列)元素列表生成线段，每次生成一根线段

    :param line_list: 线段列表
    :param seqs: 已合并的(标准特征序列)元素列表
    :return: line_list, bars_ubi: 更新后的线段列表和元素列表
    """
    up_seqs = [x for x in seqs if x.direction == 'up']
    down_seqs = [x for x in seqs if x.direction == 'down']
    if len(up_seqs) < 3 or len(down_seqs) < 3:
        return line_list, seqs

    # 查找线段
    if not line_list or len(line_list) < 1:
        # 第一个线段的查找
        up_seqs_fxs = check_fxs_seq(up_seqs)
        down_seqs_fxs = check_fxs_seq(down_seqs)

        # fxs = check_fxs(bars_ubi)
        if not up_seqs_fxs or not down_seqs_fxs:
            return line_list, seqs

        # 合并两个分型列表
        all_fxs = []
        pos1 = 0
        pos2 = 0
        while pos1 < len(up_seqs_fxs) or pos2 < len(down_seqs_fxs):
            if pos2 == len(down_seqs_fxs) or (
                    pos1 < len(up_seqs_fxs) and up_seqs_fxs[pos1].dt < down_seqs_fxs[pos2].dt):
                all_fxs.append(up_seqs_fxs[pos1])
                pos1 = pos1 + 1
            else:
                all_fxs.append(down_seqs_fxs[pos2])
                pos2 = pos2 + 1

        fx_a = all_fxs[0]
        fxs_a = [x for x in all_fxs if x.mark == fx_a.mark]
        for fx in fxs_a:
            if (fx_a.mark == 'D' and fx.low <= fx_a.low) \
                    or (fx_a.mark == 'G' and fx.high >= fx_a.high):
                fx_a = fx
        seqs = [x for x in seqs if x.start_dt >= fx_a.elements[0].start_dt]

        line, seqs_ = check_line(seqs)
        if isinstance(line, Line):
            line_list.append(line)
        seqs = seqs_
        return line_list, seqs

    last_line = line_list[-1]

    # 如果上一线段被破坏，将上一线段的bars与bars_ubi进行合并
    min_low_ubi = min([x.low for x in seqs[2:]])
    max_high_ubi = max([x.high for x in seqs[2:]])

    if last_line.direction == 'up' and max_high_ubi > last_line.high:
        if min_low_ubi < last_line.low and len(line_list) > 2:
            seqs = line_list[-2].seqs \
                   + [x for x in line_list[-1].seqs if x.start_dt > line_list[-2].seqs[-1].end_dt] \
                   + [x for x in seqs if x.start_dt > line_list[-1].seqs[-1].end_dt]
            line_list.pop(-1)
            line_list.pop(-1)
        else:
            seqs = last_line.seqs + [x for x in seqs if x.start_dt > last_line.seqs[-1].start_dt]
            line_list.pop(-1)
    elif last_line.direction == 'down' and min_low_ubi < last_line.low:
        if max_high_ubi > last_line.high and len(line_list) > 2:
            seqs = line_list[-2].seqs \
                   + [x for x in line_list[-1].seqs if x.start_dt > line_list[-2].seqs[-1].end_dt] \
                   + [x for x in seqs if x.start_dt > line_list[-1].seqs[-1].end_dt]
            line_list.pop(-1)
            line_list.pop(-1)
        else:
            seqs = last_line.seqs + [x for x in seqs if x.start_dt > last_line.seqs[-1].end_dt]
            line_list.pop(-1)
    else:
        seqs = seqs

    if len(seqs) > 300:
        print("{}  未完成笔延伸超长，延伸数量: {}".format(last_line.symbol, len(seqs)))
    line, seqs_ = check_line(seqs)
    seqs = seqs_
    if isinstance(line, Line):
        line_list.append(line)

    return line_list, seqs


def check_line(seqs: List[Seq]):
    """输入一串无包含关系特征序列，查找其中的一根线段"""
    up_seqs = [x for x in seqs if x.direction == 'up']
    down_seqs = [x for x in seqs if x.direction == 'down']
    up_fxs = check_fxs_seq(up_seqs)
    down_fxs = check_fxs_seq(down_seqs)
    if len(up_fxs) < 2 or len(down_fxs) < 2:
        return None, seqs
    fx_a = up_fxs[0] if up_fxs[0].dt < down_fxs[0].dt else down_fxs[0]
    try:
        if fx_a.mark == 'D':
            direction = 'up'
            fxs_b = [x for x in down_fxs if x.mark == 'G' and x.dt > fx_a.dt and x.fx > fx_a.fx]
            if not fxs_b:
                return None, seqs
            fx_b = fxs_b[0]
            for fx in fxs_b:
                if fx.high >= fx_b.high:
                    fx_b = fx
        elif fx_a.mark == 'G':
            direction = 'down'
            fxs_b = [x for x in up_fxs if x.mark == 'D' and x.dt > fx_a.dt and x.fx < fx_a.fx]
            if not fxs_b:
                return None, seqs
            fx_b = fxs_b[0]
            for fx in fxs_b[1:]:
                if fx.low <= fx_b.low:
                    fx_b = fx
        else:
            raise ValueError
    except:
        traceback.print_exc()
        return None, seqs

    seqs_a = [x for x in seqs if fx_a.elements[0].start_dt <= x.start_dt <= fx_b.elements[2].start_dt]
    seqs_b = [x for x in seqs if x.start_dt >= fx_b.elements[0].start_dt]

    # 判断fx_a和fx_b价格区间是否存在包含关系
    ab_include = (fx_a.high > fx_b.high and fx_a.low < fx_b.low) or (fx_a.high < fx_b.high and fx_a.low > fx_b.low)

    if len(seqs_a) >= 7 and not ab_include:
        # 计算笔的相关属性
        power_price = round(abs(fx_b.fx - fx_a.fx), 2)

        # 生成Line对象
        if direction == 'up':
            start_dt = fx_a.elements[1].start_dt if fx_a.direction == 'up' else fx_a.elements[1].end_dt
            end_dt = fx_b.elements[1].start_dt if fx_b.direction == 'down' else fx_b.elements[1].end_dt
        else:
            start_dt = fx_a.elements[1].start_dt if fx_a.direction == 'down' else fx_a.elements[1].end_dt
            end_dt = fx_b.elements[1].start_dt if fx_b.direction == 'up' else fx_b.elements[1].end_dt
        high = fx_a.fx if direction == 'down' else fx_b.fx
        low = fx_a.fx if direction == 'up' else fx_b.fx

        line = Line(symbol=fx_a.symbol, freq=fx_a.freq, id=-1, direction=direction, start_dt=start_dt, end_dt=end_dt,
                    high=high, low=low, power=power_price, seqs=seqs_a, fx_a=fx_a, fx_b=fx_b)

        return line, seqs_b
    else:
        return None, seqs


def generate_hub(hubs: List[Hub], bi_list: List[BI], point_list: List[Point]):
    """根据笔列表生成中枢，每次生成一个中枢

    :param hubs: 中枢列表
    :param bi_list: 笔列表
    :return: hubs, bi_list: 更新后的中枢列表和笔列表
    """
    if len(bi_list) < 3:
        return hubs, bi_list, point_list

    # 获取上一个中枢或者第一个中枢
    if not hubs or len(hubs) < 1:
        last_hub, bi_list = check_hub(bi_list, None)
        if last_hub:
            hubs.append(last_hub)
    else:
        last_hub = hubs[-1]

    if not last_hub:
        return hubs, bi_list, point_list

    # 上一个中枢延伸
    pos = 0
    while pos < len(bi_list):
        if not last_hub.leave:
            last_hub.leave = bi_list[pos]
        if last_hub.leave.fx_a.dt == bi_list[pos].fx_a.dt:
            pos += 1
            continue
        if last_hub.ZD > bi_list[pos].high or last_hub.ZG < bi_list[pos].low:
            if last_hub.ZD > bi_list[pos].high:
                # 中枢结束，形成三卖
                fx = bi_list[pos].fx_b if bi_list[pos].direction == 'up' else bi_list[pos].fx_a
                if len(point_list) == 0 or (len(point_list) > 0 and point_list[-1].dt < fx.dt):
                    point_list.append(
                        Point(id=-1, symbol=fx.symbol, freq=fx.freq, dt=fx.dt, type='S3',
                              high=fx.elements[1].high, low=fx.elements[1].low))
            elif last_hub.ZG < bi_list[pos].low:
                # 中枢结束，形成三买
                fx = bi_list[pos].fx_b if bi_list[pos].direction == 'down' else bi_list[pos].fx_a
                if len(point_list) == 0 or (len(point_list) > 0 and point_list[-1].dt < fx.dt):
                    point_list.append(
                        Point(id=-1, symbol=fx.symbol, freq=fx.freq, dt=fx.dt, type='B3',
                              high=fx.elements[1].high, low=fx.elements[1].low))
            break
        last_hub.elements.append(bi_list[pos])
        last_hub.GG = max([x.high for x in last_hub.elements])
        last_hub.DD = min([x.low for x in last_hub.elements])
        last_hub.leave = bi_list[pos + 1] if pos < len(bi_list) - 1 else None
        pos += 2

    # 计算当前中枢
    cur_hub, bi_list = check_hub([x for x in bi_list if x.fx_a.dt >= last_hub.elements[-1].fx_b.dt], last_hub)
    if cur_hub and not cur_hub.entry:
        cur_hub.entry = last_hub.leave

    if cur_hub:
        hubs.append(cur_hub)

    return hubs, bi_list, point_list


def check_hub(bi_list: List[BI], last_hub):
    """输入一串笔，查找其中的第一个中枢

    :param last_hub:
    :param bi_list: 笔列表
    :return: hub, bi_list: 查找到的中枢，和更新后的笔列表
    """
    start_idx = -1
    for i in range(len(bi_list) - 3):
        bi1 = bi_list[i]
        bi3 = bi_list[i + 2]
        zg = min(bi1.high, bi3.high)
        zd = max(bi1.low, bi3.low)
        if zg > zd:
            # 检验中枢方向是否正确
            if last_hub is not None:
                if (bi1.direction == 'down' and zg < last_hub.ZG) or (bi1.direction == 'up' and zd > last_hub.ZD):
                    continue
            # 记录中枢开始位置
            start_idx = i
            break
    if start_idx < 0:
        return None, bi_list

    bi1 = bi_list[start_idx]
    bi3 = bi_list[start_idx + 2]
    entry = None if start_idx == 0 else bi_list[start_idx - 1]
    if entry is None and last_hub is not None:
        entry = last_hub.elements[-1]
    leave = None if start_idx >= len(bi_list) - 3 else bi_list[start_idx + 3]

    hub = Hub(id=-1, symbol=bi1.symbol, freq=bi1.freq, ZG=min(bi1.high, bi3.high), ZD=max(bi1.low, bi3.low),
              GG=max(bi1.high, bi3.high), DD=min(bi1.low, bi3.low), entry=entry, leave=leave, elements=[bi1, bi3])

    bi_list = [x for x in bi_list if x.fx_a.dt >= hub.elements[-1].fx_b.dt]

    return hub, bi_list


def get_sub_span(bis: List[BI], start_dt: [datetime, str], end_dt: [datetime, str], direction: Direction) -> List[BI]:
    """获取子区间（这是进行多级别联立分析的关键步骤）

    :param bis: 笔的列表
    :param start_dt: 子区间开始时间
    :param end_dt: 子区间结束时间
    :param direction: 方向
    :return: 子区间
    """
    start_dt = pd.to_datetime(start_dt)
    end_dt = pd.to_datetime(end_dt)
    sub = []
    for bi in bis:
        if bi.fx_b.dt > start_dt > bi.fx_a.dt:
            sub.append(bi)
        elif start_dt <= bi.fx_a.dt < bi.fx_b.dt <= end_dt:
            sub.append(bi)
        elif bi.fx_a.dt < end_dt < bi.fx_b.dt:
            sub.append(bi)
        else:
            continue

    if len(sub) > 0 and sub[0].direction != direction:
        sub = sub[1:]
    if len(sub) > 0 and sub[-1].direction != direction:
        sub = sub[:-1]
    return sub


def get_sub_bis(bis: List[BI], bi: BI) -> List[BI]:
    """获取大级别笔对象对应的小级别笔走势

    :param bis: 小级别笔列表
    :param bi: 大级别笔对象
    :return:
    """
    sub_bis = get_sub_span(bis, start_dt=bi.fx_a.dt, end_dt=bi.fx_b.dt, direction=bi.direction)
    if not sub_bis:
        return []
    return sub_bis


class CZSC:
    def __init__(self, bars: List[RawBar], max_bi_count=50, get_signals: Callable = None):
        """

        :param bars: K线数据
        :param get_signals: 自定义的信号计算函数
        :param max_bi_count: 最大保存的笔数量
            默认值为 50，仅使用内置的信号和因子，不需要调整这个参数。
            如果进行新的信号计算需要用到更多的笔，可以适当调大这个参数。
        """
        self.max_bi_count = max_bi_count
        self.bars_raw = []  # 原始K线序列
        self.bars_ubi = []  # 未完成笔的无包含K线序列
        self.bi_list: List[BI] = []
        self.symbol = bars[0].symbol
        self.freq = bars[0].freq
        self.get_signals = get_signals
        self.signals = None

        for bar in bars:
            self.update(bar)

    def __repr__(self):
        return "<CZSC~{}~{}>".format(self.symbol, self.freq.value)

    def __update_bi(self):
        bars_ubi = self.bars_ubi
        if len(bars_ubi) < 3:
            return

        # 查找笔
        if not self.bi_list:
            # 第一个笔的查找
            fxs = check_fxs(bars_ubi)
            if not fxs:
                return

            fx_a = fxs[0]
            fxs_a = [x for x in fxs if x.mark == fx_a.mark]
            for fx in fxs_a:
                if (fx_a.mark == Mark.D and fx.low <= fx_a.low) \
                        or (fx_a.mark == Mark.G and fx.high >= fx_a.high):
                    fx_a = fx
            bars_ubi = [x for x in bars_ubi if x.dt >= fx_a.elements[0].dt]

            bi, bars_ubi_ = check_bi(bars_ubi)
            if isinstance(bi, BI):
                self.bi_list.append(bi)
            self.bars_ubi = bars_ubi_
            return

        last_bi = self.bi_list[-1]

        # 如果上一笔被破坏，将上一笔的bars与bars_ubi进行合并
        min_low_ubi = min([x.low for x in bars_ubi[2:]])
        max_high_ubi = max([x.high for x in bars_ubi[2:]])

        if last_bi.direction == Direction.Up and max_high_ubi > last_bi.high:
            if min_low_ubi < last_bi.low and len(self.bi_list) > 2:
                bars_ubi_a = self.bi_list[-2].bars \
                             + [x for x in self.bi_list[-1].bars if x.dt > self.bi_list[-2].bars[-1].dt] \
                             + [x for x in bars_ubi if x.dt > self.bi_list[-1].bars[-1].dt]
                self.bi_list.pop(-1)
                self.bi_list.pop(-1)
            else:
                bars_ubi_a = last_bi.bars + [x for x in bars_ubi if x.dt > last_bi.bars[-1].dt]
                self.bi_list.pop(-1)
        elif last_bi.direction == Direction.Down and min_low_ubi < last_bi.low:
            if max_high_ubi > last_bi.high and len(self.bi_list) > 2:
                bars_ubi_a = self.bi_list[-2].bars \
                             + [x for x in self.bi_list[-1].bars if x.dt > self.bi_list[-2].bars[-1].dt] \
                             + [x for x in bars_ubi if x.dt > self.bi_list[-1].bars[-1].dt]
                self.bi_list.pop(-1)
                self.bi_list.pop(-1)
            else:
                bars_ubi_a = last_bi.bars + [x for x in bars_ubi if x.dt > last_bi.bars[-1].dt]
                self.bi_list.pop(-1)
        else:
            bars_ubi_a = bars_ubi

        if len(bars_ubi_a) > 300:
            print("{} - {} 未完成笔延伸超长，延伸数量: {}".format(self.symbol, self.freq, len(bars_ubi_a)))
        bi, bars_ubi_ = check_bi(bars_ubi_a)
        self.bars_ubi = bars_ubi_
        if isinstance(bi, BI):
            self.bi_list.append(bi)

    def update(self, bar: RawBar):
        """更新分析结果

        :param bar: 单根K线对象
        """
        # 更新K线序列
        if not self.bars_raw or bar.dt != self.bars_raw[-1].dt:
            self.bars_raw.append(bar)
            last_bars = [bar]
        else:
            self.bars_raw[-1] = bar
            last_bars = self.bars_ubi[-1].elements
            last_bars[-1] = bar
            self.bars_ubi.pop(-1)

        # 去除包含关系
        bars_ubi = self.bars_ubi
        for bar in last_bars:
            if len(bars_ubi) < 2:
                bars_ubi.append(NewBar(symbol=bar.symbol, id=bar.id, freq=bar.freq, dt=bar.dt,
                                       open=bar.open, close=bar.close,
                                       high=bar.high, low=bar.low, vol=bar.vol, elements=[bar]))
            else:
                k1, k2 = bars_ubi[-2:]
                has_include, k3 = remove_include(k1, k2, bar)
                if has_include:
                    bars_ubi[-1] = k3
                else:
                    bars_ubi.append(k3)
        self.bars_ubi = bars_ubi

        # 更新笔
        self.__update_bi()
        self.bi_list = self.bi_list[-self.max_bi_count:]
        if self.bi_list:
            sdt = self.bi_list[0].fx_a.elements[0].dt
            s_index = 0
            for i, bar in enumerate(self.bars_raw):
                if bar.dt >= sdt:
                    s_index = i
                    break
            self.bars_raw = self.bars_raw[s_index:]

        if self.get_signals:
            self.signals = self.get_signals(c=self)
        else:
            self.signals = OrderedDict()

    def to_echarts(self, width: str = "1400px", height: str = '580px'):
        kline = [x.__dict__ for x in self.bars_raw]
        if len(self.bi_list) > 0:
            bi = [{'dt': x.fx_a.dt, "bi": x.fx_a.fx} for x in self.bi_list] + \
                 [{'dt': self.bi_list[-1].fx_b.dt, "bi": self.bi_list[-1].fx_b.fx}]
        else:
            bi = None
        chart = kline_pro(kline, bi=bi, width=width, height=height, title="{}-{}".format(self.symbol, self.freq.value))
        return chart

    def open_in_browser(self, width: str = "1400px", height: str = '580px'):
        """直接在浏览器中打开分析结果

        :param width: 图表宽度
        :param height: 图表高度
        :return:
        """
        home_path = os.path.expanduser("~")
        file_html = os.path.join(home_path, "temp_czsc.html")
        chart = self.to_echarts(width, height)
        chart.render(file_html)
        webbrowser.open(file_html)

    @property
    def finished_bis(self) -> List[BI]:
        """返回当下基本确认完成的笔列表"""
        if not self.bi_list:
            return []

        min_ubi = min([x.low for x in self.bars_ubi])
        max_ubi = max([x.high for x in self.bars_ubi])
        if (self.bi_list[-1].direction == Direction.Down and min_ubi >= self.bi_list[-1].low) \
                or (self.bi_list[-1].direction == Direction.Up and max_ubi <= self.bi_list[-1].high):
            bis = self.bi_list
        else:
            bis = self.bi_list[:-1]
        return bis


class CzscTrader:
    """缠中说禅技术分析理论之多级别联立交易决策类"""

    def __init__(self, kg: KlineGenerator, get_signals: Callable, events: List[Event] = None):
        """

        :param kg: K线合成器
        :param get_signals: 自定义的单级别信号计算函数
        :param events: 自定义的交易事件组合
        """
        self.name = "CzscTrader"
        self.kg = kg
        self.freqs = kg.freqs
        self.events = events
        self.op = dict()

        klines = self.kg.get_klines({k: 3000 for k in self.freqs})
        self.kas = {k: CZSC(klines[k], max_bi_count=50, get_signals=get_signals) for k in klines.keys()}
        self.symbol = self.kas["1分钟"].symbol
        self.end_dt = self.kas["1分钟"].bars_raw[-1].dt
        self.latest_price = self.kas["1分钟"].bars_raw[-1].close
        self.s = self._cal_signals()

        # cache 中会缓存一些实盘交易中需要的信息
        self.cache = OrderedDict({
            "last_op": None,  # 最近一个操作类型
            "long_open_price": None,  # 多仓开仓价格
            "long_max_high": None,  # 多仓开仓后的最高价
            "long_open_k1_id": None,  # 多仓开仓时的1分钟K线ID
            "short_open_price": None,  # 空仓开仓价格
            "short_min_low": None,  # 空仓开仓后的最低价
            "short_open_k1_id": None,  # 空仓开仓后的1分钟K线ID
        })

    def __repr__(self):
        return "<{} for {}>".format(self.name, self.symbol)

    def take_snapshot(self, file_html=None, width="1400px", height="580px"):
        """获取快照

        :param file_html: str
            交易快照保存的 html 文件名
        :param width: str
            图表宽度
        :param height: str
            图表高度
        :return:
        """
        tab = Tab(page_title="{}@{}".format(self.symbol, self.end_dt.strftime("%Y-%m-%d %H:%M")))
        for freq in self.freqs:
            chart = self.kas[freq].to_echarts(width, height)
            tab.add(chart, freq)

        for freq in self.freqs:
            t1 = Table()
            t1.add(["名称", "数据"], [[k, v] for k, v in self.s.items() if k.startswith("{}_".format(freq))])
            t1.set_global_opts(title_opts=ComponentTitleOpts(title="缠中说禅信号表", subtitle=""))
            tab.add(t1, "{}信号表".format(freq))

        t2 = Table()
        ths_ = [["同花顺F10", "http://basic.10jqka.com.cn/{}".format(self.symbol[:6])]]
        t2.add(["名称", "数据"], [[k, v] for k, v in self.s.items() if "_" not in k] + ths_)
        t2.set_global_opts(title_opts=ComponentTitleOpts(title="缠中说禅因子表", subtitle=""))
        tab.add(t2, "因子表")

        if file_html:
            tab.render(file_html)
        else:
            return tab

    def open_in_browser(self, width="1400px", height="580px"):
        """直接在浏览器中打开分析结果"""
        home_path = os.path.expanduser("~")
        file_html = os.path.join(home_path, "temp_czsc_factors.html")
        self.take_snapshot(file_html, width, height)
        webbrowser.open(file_html)

    def _cal_signals(self):
        """计算信号"""
        s = OrderedDict()
        for freq, ks in self.kas.items():
            s.update(ks.signals)

        s.update(self.kas['1分钟'].bars_raw[-1].__dict__)
        return s

    def check_operate(self, bar: RawBar, stoploss: float = 0.1, timeout: int = 1000) -> dict:
        """更新信号，计算下一个操作动作

        :param timeout: 超时退出参数，数值表示持仓1分钟K线数量
        :param stoploss: 止损退出参数，0.1 表示10个点止损
        :param bar: 单根K线对象
        :return: 操作提示
        """
        self.kg.update(bar)
        klines_one = self.kg.get_klines({freq: 1 for freq in self.freqs})

        for freq, klines_ in klines_one.items():
            self.kas[freq].update(klines_[-1])

        self.symbol = self.kas["1分钟"].symbol
        self.end_dt = self.kas["1分钟"].bars_raw[-1].dt
        self.latest_price = self.kas["1分钟"].bars_raw[-1].close
        self.s = self._cal_signals()

        # 遍历 events，获得 operate
        op = {"operate": Operate.HO.value, 'symbol': self.symbol, 'dt': self.end_dt,
              'price': self.latest_price, "desc": '', 'bid': self.kg.m1[-1].id}
        if self.events:
            for event in self.events:
                m, f = event.is_match(self.s)
                if m:
                    op['operate'] = event.operate.value
                    op['desc'] = f"{event.name}@{f}"
                    break

        # 判断是否达到异常退出条件
        if op['operate'] == Operate.HO.value and self.cache['last_op']:
            if self.cache['last_op'] == Operate.LO.value:
                assert self.cache['long_open_price']
                assert self.cache['long_open_k1_id']
                if self.latest_price < self.cache.get('long_open_price', 0) * (1 - stoploss):
                    op['operate'] = Operate.LE.value
                    op['desc'] = f"long_pos_stoploss_{stoploss}"

                if self.kg.m1[-1].id - self.cache.get('long_open_k1_id', 99999999999) > timeout:
                    op['operate'] = Operate.LE.value
                    op['desc'] = f"long_pos_timeout_{timeout}"

            if self.cache['last_op'] == Operate.SO.value:
                assert self.cache['short_open_price']
                assert self.cache['short_open_k1_id']
                self.cache['short_min_low'] = min(self.latest_price, self.cache['short_min_low'])
                if self.latest_price > self.cache.get('long_open_price', 10000000000) * (1 + stoploss):
                    op['operate'] = Operate.SE.value
                    op['desc'] = f"short_pos_stoploss_{stoploss}"

                if self.kg.m1[-1].id - self.cache.get('short_open_k1_id', 99999999999) > timeout:
                    op['operate'] = Operate.SE.value
                    op['desc'] = f"short_pos_timeout_{timeout}"

        # update cache
        if op['operate'] == Operate.LE.value:
            self.cache.update({
                "long_open_price": None,
                "long_open_k1_id": None,
                "long_max_high": None,
                "last_op": Operate.LE.value
            })
        elif op['operate'] == Operate.LO.value:
            self.cache.update({
                "long_open_price": self.latest_price,
                "long_open_k1_id": self.kg.m1[-1].id,
                "long_max_high": self.latest_price,
                "last_op": Operate.LO.value
            })
        elif op['operate'] == Operate.SE.value:
            self.cache.update({
                "short_open_price": None,
                "short_open_k1_id": None,
                "short_min_low": None,
                "last_op": Operate.SE.value
            })
        elif op['operate'] == Operate.SO.value:
            self.cache.update({
                "short_open_price": self.latest_price,
                "short_open_k1_id": self.kg.m1[-1].id,
                "short_min_low": self.latest_price,
                "last_op": Operate.SO.value
            })
        else:
            assert op['operate'] == Operate.HO.value
            if self.cache['last_op'] and self.cache['last_op'] == Operate.LO.value:
                assert self.cache['long_open_price']
                assert self.cache['long_open_k1_id']
                self.cache['long_max_high'] = max(self.latest_price, self.cache['long_max_high'])

            if self.cache['last_op'] and self.cache['last_op'] == Operate.SO.value:
                assert self.cache['short_open_price']
                assert self.cache['short_open_k1_id']
                self.cache['short_min_low'] = min(self.latest_price, self.cache['short_min_low'])

        self.op = op
        return op
