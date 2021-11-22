# 首次使用需要设置聚宽账户
from czsc.data.jq import set_token
set_token("18800166629", '123QWEasdZXC')

from datetime import datetime
from typing import List
from czsc.data.jq import get_kline, get_index_stocks
import czsc
from czsc.analyze import CZSC
from czsc.signals import get_default_signals
from czsc.signals import get_s_three_bi, get_selector_signals
#from czsc.enum import signals

assert czsc.__version__ == '0.7.9'

def is_third_buy(symbol):
    """判断一个股票现在是否有日线三买"""
    bars = get_kline(symbol, freq="D", end_date=datetime.now(), count=1000)
    c = CZSC(bars, get_signals=get_default_signals)
    print(c.signals)
    print(c.bi_list)
    # print(get_s_three_bi(c, 10))
    # c.open_in_browser(width="1400px", height="580px")
    # 在这里判断是否有五笔三买形态，也可以换成自己感兴趣的形态
    # if c.signals['倒1五笔'] in [signals.X5LB0.value, signals.X5LB1.value]:
    #     return True
    # else:
    #     return False


if __name__ == '__main__':
    # 获取上证50最新成分股列表，这里可以换成自己的股票池
    symbols: List = get_index_stocks("000016.XSHG")
    # print(symbols)
    is_third_buy(symbols[0])
    # for symbol in symbols:
    #     try:
    #         if is_third_buy(symbol):
    #             print("{} - 日线三买".format(symbol))
    #     except:
    #         print("{} - 执行失败".format(symbol))
