"""数据采集模块"""
import symbol

import logging
import tushare as ts
import requests
import time
import pandas as pd
import sqlite3
import talib as tl
import json
from pathlib import Path

from .MysqlUtils import MysqlUtils
from .dataOperation import FileOperation, formerComplexRights
from concurrent.futures import ThreadPoolExecutor
import pymysql
from sqlalchemy import create_engine
from .jukuan import Data
from datetime import datetime

from ..czsc import CZSC
from ..czsc.data.jq import get_kline
from ..czsc.enum import Freq
from ..czsc.objects import RawBar
from ..czsc.signals import get_default_signals
from ..czsc.utils.jsonEncoder import BiJSONEncoder
from ..czsc.utils.kline_generator import bar_end_time

db_con = MysqlUtils('127.0.0.1', 3307, 'root', '123456', 'chanlun', 'utf8')

TOKEN_PATH = Path(__file__).parent.parent.joinpath("DataBase", "token.txt")  # 用户密钥位置

# 计算线段相关变量（支持增量计算）
stroke_i = 0  # stroke_i指向strokeList中元素（全局变量）
stroke_combine = []  # 合并后的特征序列端点列表,在出现线段转折时清空
strokesList = []  # 笔端点列表
lineList = []  # 线段端点列表
stroke_j = 1  # j用于合并，指向_combine中待判断合并的特征序列的起始端(线段端点的后一个元素（1）)，只有未出现包含时才更新j,且指向最后一个特征序列起始端
jLen = -1  # j及其后元素个数
up = 0  # up=1表示向上笔，up=-1表示向下笔，为0表示开始新一轮线段走势判断
last_i = 0  # 加入线段端点的最后一个i，用于在出现转折点时还原i值
last_line = []  # 更新线段端点时的旧端点，用于在出现包含时还原端点

pivotList = []  # 中枢列表[[日期1，日期2，中枢低点，中枢高点]]
currentPivot = []  # 暂存中枢前三段的四个顶点纵坐标，用于排序获得中枢低点和高点
pivot_i = 1  # pivot_i为lineList中元素下标（全局变量），用于判断中枢的延申或是新生
startDate = ''  # 中枢开始日期
endDate = ''  # 中枢结束日期
pivot_j = 0  # pivot_j为最近一个中枢的元素下标

buy_list = []  # 买点列表[[日期，值，类型]]
sell_list = []  # 卖点列表[[日期，值，类型]]
buy_list_history = []  # 买点列表[[日期，值，类型]]，三类买点分别为B1、B2、B3
sell_list_history = []  # 卖点列表[[日期，值，类型]]，三类卖点分别为S1、S2、S3
update_buy_and_sell = []  # 判断更新买卖点列表的关键位置[[日期，值]]

stock_id = ''
# mysqlCon = create_engine('mysql+pymysql://root:123456@127.0.0.1:3307/chanlun?charset=utf8')
mysqlCon = create_engine('mysql+pymysql://root:5207158@123.56.222.52:3306/chanlun?charset=utf8')
source = Data()
source.refreshToken()
db = pymysql.Connect(
    host='123.56.222.52',
    port=3306,
    user='root',
    passwd='5207158',
    db='chanlun',
    charset='utf8'
)
freq_map = {
    "1m": "1分钟",
    "5m": "5分钟",
    "15m": "15分钟",
    "30m": "30分钟",
    "60m": "60分钟",
    "1d": "日线"
}

freq_obj_map = {'1m': Freq.F1, '5m': Freq.F5, '15m': Freq.F15, '30m': Freq.F30,
                '60m': Freq.F60, '1d': Freq.D, '1w': Freq.W, '1M': Freq.M}


class DataSource:
    """
    Tushare数据源。

    :param token: tushare数据接口密钥
    """

    def __init__(self, token=''):
        if token:
            ts.set_token(token)
        self.pro = ts.pro_api()

    def getStockList(self):
        """获取上市股票列表。"""
        data = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,market')
        return data

    def getTradeDate(self, last=0):
        """"
        获取交易日期,判断今日是否交易。

        :param last: last=1获取最后交易日期 last=0判断当天是否交易
        :return last=1: 返回最后交易日(str) last=0: 返回当天是否交易(bool)
        :rtype: str or bool
        """
        date = getCurrentTime(TorD="date")
        if last:  # 获取最后交易日期
            return self.pro.trade_cal(end_date=date, is_open=1)["cal_date"][-1:].to_numpy()[0]
        else:  # 判断当天是否交易
            tradeOrNot = self.pro.trade_cal(start_date=date, end_date=date)["is_open"][0]
            return tradeOrNot

    def getDaily(self, Code, save=0, **kwargs):
        """
        获取日线行情及复权因子(顺序排列)。

        :param Code: 所获取股票数据的代码
        :param save: 控制是否存入数据库 ， save=1 存入数据库，否则仅返回数据
        :param date: 可选参数，与start参数不可同时使用，获得指定日期的数据
        :param start: 可选参数，与date参数不可同时使用，获取从指定日期到最近日期的数据
        :type Code: str
        :type save: bool
        :type date: str
        :type start: str
        :return: 显示返回日线行情数据与复权因子数据（仅当save=0时）
        :rtype: dataframe
        """
        if Code[0] == '6':
            ts_code = Code + '.SH'
        else:
            ts_code = Code + '.SZ'
        if "date" in kwargs:
            Data = self.pro.daily(ts_code=ts_code, trade_date=kwargs["date"],
                                  fields="trade_date,open,close,high,low,vol")  # 获取日线行情
            factor = self.pro.adj_factor(ts_code=ts_code, trade_date=kwargs["date"], fields="adj_factor")  # 获取复权因子
        elif "start" in kwargs:
            Data = self.pro.daily(ts_code=ts_code, start_date=kwargs["start"], end_date=kwargs["end"],
                                  fields="trade_date,open,close,high,low,vol")  # 获取日线行情
            factor = self.pro.adj_factor(ts_code=ts_code, start_date=kwargs["start"], end_date=kwargs["end"],
                                         fields="adj_factor")  # 获取复权因子
        else:
            Data = self.pro.daily(ts_code=ts_code, fields="trade_date,open,close,high,low,vol")  # 获取日线行情
            factor = self.pro.adj_factor(ts_code=ts_code, fields="adj_factor")  # 获取复权因子
        Data = Data.join(factor, how="left", lsuffix="_left", rsuffix="_right")  # 合并
        # 按照时间顺序排列数据，并设置时间为索引
        Data.trade_date = pd.DatetimeIndex(Data.trade_date)
        Data.set_index("trade_date", drop=True, inplace=True)
        Data.sort_index(inplace=True)
        Data.index = Data.index.set_names('Date')
        recon_data = {'Open': Data.open, 'Close': Data.close, 'High': Data.high, 'Low': Data.low, 'Volume': Data.vol,
                      'Factor': Data.adj_factor}
        df_recon = pd.DataFrame(recon_data)
        if save:
            # 存入数据库
            dataBase = Path(__file__).parent.parent.joinpath("DataBase", "stockData.db")
            Connect = sqlite3.connect(dataBase)
            df_recon.to_sql(Code, Connect, if_exists="append", index=True)
            Connect.close()
        else:
            return df_recon


def getCurrentInformation(code):
    """
    获取实时数据(腾迅股票数据接口)。

    :param code: 获取股票数据的代码
    :return: 返回字典格式的实时数据
    :rtype: dict
    """
    response = requests.get(f'http://qt.gtimg.cn/q={code}')  # 腾讯股票数据接口
    data = response.text[12:-2].split("~")
    information = {
        "name": data[1],
        "code": data[2],
        "current": data[3],  # 现价
        "yesterday_closing": data[4],  # 昨收
        "today_opening": data[5],  # 今开
        "volume": data[6],  # 成交量
        "outer": data[7],  # 外盘
        "inner": data[8],  # 内盘
        "buy_1": data[9],  # 买一价
        "buy_1_count": data[10],  # 买一量
        "buy_2": data[11],  # 买二价
        "buy_2_count": data[12],  # 买二量
        "buy_3": data[13],  # 买三价
        "buy_3_count": data[14],  # 买三量
        "buy_4": data[15],  # 买四价
        "buy_4_count": data[16],  # 买四量
        "buy_5": data[17],  # 买五价
        "buy_5_count": data[18],  # 买五量
        "sell_1": data[19],  # 卖一价
        "sell_1_count": data[20],  # 卖一量
        "sell_2": data[21],  # 卖二价
        "sell_2_count": data[22],  # 卖二量
        "sell_3": data[23],  # 卖三价
        "sell_3_count": data[24],  # 卖三量
        "sell_4": data[25],  # 卖四价
        "sell_4_count": data[26],  # 卖四量
        "sell_5": data[27],  # 卖五价
        "sell_5_count": data[28],  # 卖五量
        "time": data[30][-6:-4] + ":" + data[30][-4:-2] + ":" + data[30][-2:],  # 时间
        "AD": data[31],  # 涨跌
        "AD_rate": data[32],  # 涨幅
        "highest": data[33],  # 最高价
        "lowest": data[34],  # 最低价
        "turnover_rate": data[38],  # 换手率
        "PE_rate": data[39],  # 市盈率
        "circulation_value": data[44],  # 流通市值
        "total_value": data[45],  # 总市值
        "PB_rate": data[46],  # 市净率
        "limit_up": data[47],  # 涨停价
        "limit_down": data[48],  # 跌停价
    }
    return information


def getCurrentTime(TorD='', SorI='', Sleep=0):
    """
    返回当前时间与日期或休眠。

    :param TorD: 设置返回时间或日期 TorD="time":返回时间 TorD="date":返回日期 TorD="date&time":返回时间与日期
    :param SorI: 设置返回str或int SorI="int":返回int数据，否则返回str
    :param Sleep: 设置休眠时长，仅当TorD和SorI参数为空时有效
    :type TorD: str
    :type SorI: str
    :type Sleep: int
    :return currentTime: 返回当前时间 or currentDate: 返回当前日期 or currentDateTime: 返回当前时间与日期 or 休眠，不返回信息
    :rtype: int or str
    """
    if TorD == "time":
        currentTime = time.strftime("%H%M", time.localtime())
        if SorI == "int":
            return int(currentTime)
        return currentTime
    elif TorD == "date":
        currentDate = time.strftime("%Y%m%d", time.localtime())
        if SorI == "int":
            return int(currentDate)
        return currentDate
    elif TorD == "date&time":
        currentDateTime = time.strftime("%Y-%m-%d %H:%M", time.localtime())
        return currentDateTime
    else:
        time.sleep(Sleep)


class GetDataWithThread:
    """
    多线程获取行情数据。

    :param progress: 传递高阶进度处理函数 示例: progress=self.setProgress
    :param text: 传递高阶文本展示函数 示例: text=self.setText
    :type progress: function
    :type text: function
    """

    def __init__(self, progress=None, text=None):
        self.errorList = []
        self.start = ""
        self.end = ""
        self.date = ""
        self.length = 0
        self.currentLength = 0
        self.progress = progress
        self.text = text

    def _mapFunc(self, Code):
        """线程池执行内容。"""
        try:
            # if self.date:
            #    DataSource().getDaily(Code, save=1, date=self.date)
            # elif self.start:
            #    DataSource().getDaily(Code, save=1, start=self.start, end=self.end)
            # else:
            #    DataSource().getDaily(Code, save=1)
            # self.currentLength += 1
            # progressNum = 15 + self.currentLength * 80 // self.length
            # self.progress(progressNum)
            self.text("获取成功: " + Code)
        except:  # 搜集获取失败股票代码
            self.text("获取失败: " + Code)
            self.errorList.append(Code)

    def _threadContent(self, code_list, workers):
        """线程池配置"""
        with ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(self._mapFunc, code_list)

    def startThread(self, filename=Path(__file__).parent.parent.joinpath("DataBase", "stockList.json"), date='',
                    start='', end='', workers=16, List=()):
        """调用tushare接口，开启线程池获取数据，并记录获取失败股票列表，并重新尝试获取数据"""
        begin_time = time.perf_counter()
        self.date = date
        self.start = start
        self.end = end
        self.text("线程数: " + str(workers))
        time.sleep(0.5)
        if List:  # 直接从列表读取数据
            stock_index = List
        else:  # 从json文件读取数据
            stock_index = FileOperation.loadJsonFile(filename)  # 待获取数据的股票列表
        self.errorList = [stock["symbol"] for stock in stock_index]
        self.length = len(self.errorList)  # 需要下载股票数量

        i = 2  # 记录重新尝试获取数据的次数
        self.text("第 1 轮尝试:" + str(self.errorList))
        while self.errorList:  # 若采集数据失败，将重新尝试获取失败数据
            if i > 10:
                warning = "获取数据失败，请检查您的系统配置与网络环境并稍后重试。\n" + str(self.errorList)
                self.text(warning)
                FileOperation.logOperation(warning, warning=1)  # 输出出错股票列表至日志
                break
            codeList = self.errorList
            self.errorList = []
            self._threadContent(codeList, workers)
            self.text(f"第 {i} 轮尝试:" + str(self.errorList))
            i += 1
        end_time = time.perf_counter()
        run_time = end_time - begin_time
        self.text("已获得所有新增数据。总共用时：" + str(run_time) + "\n")
        FileOperation.logOperation('已获得所有新增数据。总共用时：%s' % run_time)


def get_db_bi_list(freq, symbol):
    bi_list = []
    try:
        raw_bi_list = db_con.get_all('select * from cl_stroke where stock_id=\'%s\' and level=\'%s\'' % (symbol, freq))
        if len(raw_bi_list) == 0:
            return bi_list
        # 起始位置
        price = raw_bi_list[0][9] if raw_bi_list[0][5] == 'down' else raw_bi_list[0][10]
        bi_list.append([raw_bi_list[0][6].strftime("%Y-%m-%d %H:%M:%S"), price])
        for row in raw_bi_list:
            direction = row[5]
            price = row[9] if direction == 'up' else row[10]
            bi_list.append([row[7].strftime("%Y-%m-%d %H:%M:%S"), price])
    except Exception as e:
        logging.exception(e)

    return bi_list


def get_db_zh_list(freq, symbol):
    rePivotList = "["
    try:
        raw_zh_list = db_con.get_all(
            'select * from cl_omphalos where stock_id=\'%s\' and level=\'%s\'' % (symbol, freq))
        for Item in raw_zh_list:
            rePivotList += "[{coord: ['%s', %s]},{coord: ['%s', %s]}]," % (Item[3], Item[6], Item[4], Item[5])
    except Exception as e:
        logging.exception(e)
    rePivotList += "]"

    return rePivotList


def getDataFromMysqlDB(freq, stockId, MA=0, stroke_with_new=1, pivot_with_stroke=1, JS=1, Former=0):
    """
    从数据库获取历史数据并计算相关指标,以js格式返回至前端表格，或是以list格式返回至选股模块。

    :param console: 数据库命令
    :param MA: 是否计算ma，ma=1时计算
    :param stroke_with_new: 是否使用新笔计算线段
    :param pivot_with_stroke: 是否使用笔计算中枢
    :param JS: 是否返回JavaScript数据，默认为1，若被选股模块调用，则JS=0
    :return: JavaScript代码格式数据或列表
    :rtype: str or list
    """
    global stroke_i, stroke_j, jLen, stroke_combine, strokesList, lineList, up, last_i, last_line, \
        pivotList, currentPivot, pivot_i, pivot_j, startDate, endDate, buy_list, sell_list, \
        buy_list_history, sell_list_history, update_buy_and_sell, mysqlCon, stock_id

    # dataBase = Path(__file__).parent.parent.joinpath("DataBase", "stockData.db")  # 股票数据库
    # Connect = sqlite3.connect(dataBase)
    # rawData = pd.read_sql('select * from "600809"', Connect)

    # 获取聚宽数据
    # if freq=='1min':
    #     # end_date = pd.to_datetime(datetime.now()).strftime("%Y-%m-%d")
    #     rawData = source.get_bars(stockId, 2000, datetime.now(), freq)
    #     rawData.columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume', 'Money']
    # else:
    #     rawData = pd.read_sql("select * from " + stockId.replace(".", "_") + "_" + freq, mysqlCon)
    #     rawData['Date'] = rawData['Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    rawData = pd.read_sql("select * from " + stockId.replace(".", "_") + "_" + freq, mysqlCon)
    rawData['Date'] = rawData['Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    stock_id = stockId
    # if Former:  # Former=1时计算前复权数据
    #    rawData = formerComplexRights(rawData)  # 转为前复权数据

    try:
        if JS:  # 若返回JS数据用于前端展示
            if MA:  # 若MA=1，则计算ma
                ma10 = tl.SMA(rawData['Close'], 10).to_json(orient='split')
                ma20 = tl.SMA(rawData['Close'], 20).to_json(orient='split')
                ma30 = tl.SMA(rawData['Close'], 30).to_json(orient='split')
            else:  # 否则不计算ma
                ma10 = []
                ma20 = []
                ma30 = []
            dif, dea, macd = tl.MACD(rawData['Close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
            Macd = {}
            macd *= 2
            Macd['dif'] = dif.tolist()
            Macd['dea'] = dea.tolist()
            Macd['macd'] = macd.tolist()
            Macd = json.dumps(Macd)
        k_list = rawData.loc[:, ['Date', 'High', 'Low']].values.tolist()
        combine, stroke = combineK(k_list)  # 旧笔
        combine_new, stroke_new = combineKNew(k_list)  # 新笔
        if stroke_with_new:  # 若使用新笔
            strokesList = stroke_new
        else:  # 若使用旧笔
            strokesList = stroke

        stroke_i = 0
        stroke_combine = []
        lineList = []
        stroke_j = 1
        jLen = -1
        up = 0
        last_i = 0
        last_line = []

        pivotList = []  # 中枢列表[[日期1，日期2，中枢低点，中枢高点]]
        currentPivot = []  # 暂存中枢前三段的四个顶点纵坐标，用于排序获得中枢低点和高点
        pivot_i = 1  # line_i为lineList中元素下标（全局变量）
        startDate = ''  # 中枢开始日期
        endDate = ''  # 中枢结束日期
        pivot_j = 0  # pivot_j为最近一个中枢的元素下标

        buy_list = []
        sell_list = []
        buy_list_history = []
        sell_list_history = []
        update_buy_and_sell = []

        getLine()  # 根据笔端点列表计算线段端点列表
        getPivot(pivot_with_stroke)  # 计算中枢

        # stroke = get_bi_list(rawData, freq, symbol)  # 计算czsc笔
        stroke = get_db_bi_list(freq, stockId)
        # print(stroke)
        getBuyAndSell()  # 计算买卖点

        if JS:
            # 重新组织数据格式，用于前端展示
            pivot = get_db_zh_list(freq, stockId)
            # pivot = reFormatPivotList()
            # buy, sell = reFormatBuyAndSell(buy_list, sell_list)
            buy_1, buy_2, buy_3, sell_1, sell_2, sell_3 = reFormatBuyAndSell_classifier(buy_list, sell_list, freq)

            # buy_history, sell_history = reFormatBuyAndSell(buy_list_history, sell_list_history)
            # update = reFormatUpdate(update_buy_and_sell)

        # buy_sell_data = {'id':2,'buy_1': buy_1, 'buy_2': buy_2,
        #     'buy_3': buy_3, 'sell_1': sell_1, 
        #     'sell_2': sell_2, 'sell_3': sell_3}

        # buy_sell_data = {'id':1, 'buy_1': 'buy_1', 'buy_2': 'buy_2',
        #  'buy_3': 'buy_3', 'sell_1': 'sell_1', 
        #  'sell_2': 'sell_2', 'sell_3': 'sell_3'}

        # insertSql = "INSERT INTO entanglement VALUES(?,?,?,?,?,?,?)"
        # Connect.executemany(insertSql, df_buy_sell_data)
        # Connect.commit()
        # df_buy_sell_data.to_sql('entanglement', Connect, if_exists="append", index=True)

    except Exception as e:
        # QMessageBox.warning(self, "提示", e.args[0])
        print('Error:', e.args[0])
        if JS:
            Macd = {'dif': [], 'dea': [], 'macd': []}
            Macd = json.dumps(Macd)
            # stroke = []
            stroke_new = []

            stroke_i = 0
            stroke_combine = []
            lineList = []
            stroke_j = 1
            jLen = -1
            up = 0
            last_i = 0
            last_line = []

            pivotList = []  # 中枢列表[[日期1，日期2，中枢低点，中枢高点]]
            currentPivot = []  # 暂存中枢前三段的四个顶点纵坐标，用于排序获得中枢低点和高点
            pivot_i = 1  # line_i为lineList中元素下标（全局变量）
            startDate = ''  # 中枢开始日期
            endDate = ''  # 中枢结束日期
            pivot_j = 0  # pivot_j为最近一个中枢的元素下标
            pivot = "[]"  # pivot列表的Javascript格式字符串
            # buy = "[]"
            # sell = "[]"
            # buy_history = "[]"
            # sell_history = "[]"
            # update = "[]"

    try:
        if JS:
            js = """
                var data = getData(%s["data"])
                var Macd = %s
            myChart.setOption({
                xAxis: [
                    {data:data.date},
                    {data:data.date},
                    {data:data.date}],
                series: [{
                    name: '日K',
                    type: 'candlestick',
                    itemStyle: {
                        color: '#ec0000',
                        color0: '#00da3c',
                        borderColor: '#8A0000',
                        borderColor0: '#008F28'
                    },
                    data: data.values,
                },{
                    name: 'MA10',
                    data: %s["data"],
                },{
                    name: 'MA20',
                    data: %s["data"],
                },{
                    name: 'MA30',
                    data: %s["data"],
                },{
                    name: 'MACD',
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    data: Macd["macd"],
                    itemStyle:{
                        normal:{
                            color:function(params){
                                if(params.value >0){
                                    return '#ec0000';
                                }else{
                                    return '#00da3c';
                                }
                            }
                        }
                    }
                },{
                    name: 'DIF',
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    data: Macd["dif"]
                },{
                    name: 'DEA',
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    data: Macd["dea"]
                },{
                    name: '成交量',
                    xAxisIndex: 2,
                    yAxisIndex: 2,
                    data: data.volumes
                },{
                    name: '笔',
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    data: %s
                },
                {
                    name: '一买',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "bottom"
                        }
                    }
                },{
                    name: '二买',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "bottom"
                        }
                    }
                },{
                    name: '三买',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "bottom"
                        }
                    }
                },{
                    name: '一卖',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "top"
                        }
                    }
                },{
                    name: '二卖',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "top"
                        }
                    }
                },{
                    name: '三卖',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "top"
                        }
                    }
                },{
                    name: '中枢',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markArea: {
                        data: %s
                    },
                }]
            });
                """ % (
                rawData.to_json(orient='split'), Macd, ma10, ma20, ma30, stroke, buy_1, buy_2, buy_3, sell_1, sell_2,
                sell_3, pivot)
            return js
        else:  # 选股模块调用时只需返回买卖点列表
            return buy_list, sell_list
    except:
        pass


def getDataFromDB(console, MA=0, stroke_with_new=1, pivot_with_stroke=1, JS=1, Former=0):
    """
    从数据库获取历史数据并计算相关指标,以js格式返回至前端表格，或是以list格式返回至选股模块。

    :param console: 数据库命令
    :param MA: 是否计算ma，ma=1时计算
    :param stroke_with_new: 是否使用新笔计算线段
    :param pivot_with_stroke: 是否使用笔计算中枢
    :param JS: 是否返回JavaScript数据，默认为1，若被选股模块调用，则JS=0
    :return: JavaScript代码格式数据或列表
    :rtype: str or list
    """
    global stroke_i, stroke_j, jLen, stroke_combine, strokesList, lineList, up, last_i, last_line, \
        pivotList, currentPivot, pivot_i, pivot_j, startDate, endDate, buy_list, sell_list, \
        buy_list_history, sell_list_history, update_buy_and_sell
    dataBase = Path(__file__).parent.parent.joinpath("DataBase", "stockData.db")  # 股票数据库
    Connect = sqlite3.connect(dataBase)
    rawData = pd.read_sql(console, Connect)

    if Former:  # Former=1时计算前复权数据
        rawData = formerComplexRights(rawData)  # 转为前复权数据

    try:
        if JS:  # 若返回JS数据用于前端展示
            if MA:  # 若MA=1，则计算ma
                ma10 = tl.SMA(rawData['Close'], 10).to_json(orient='split')
                ma20 = tl.SMA(rawData['Close'], 20).to_json(orient='split')
                ma30 = tl.SMA(rawData['Close'], 30).to_json(orient='split')
            else:  # 否则不计算ma
                ma10 = []
                ma20 = []
                ma30 = []
            dif, dea, macd = tl.MACD(rawData['Close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
            Macd = {}
            macd *= 2
            Macd['dif'] = dif.tolist()
            Macd['dea'] = dea.tolist()
            Macd['macd'] = macd.tolist()
            Macd = json.dumps(Macd)
        k_list = rawData.loc[:, ['Date', 'High', 'Low']].values.tolist()
        combine, stroke = combineK(k_list)  # 旧笔
        combine_new, stroke_new = combineKNew(k_list)  # 新笔
        if stroke_with_new:  # 若使用新笔
            strokesList = stroke_new
        else:  # 若使用旧笔
            strokesList = stroke

        stroke_i = 0
        stroke_combine = []
        lineList = []
        stroke_j = 1
        jLen = -1
        up = 0
        last_i = 0
        last_line = []

        pivotList = []  # 中枢列表[[日期1，日期2，中枢低点，中枢高点]]
        currentPivot = []  # 暂存中枢前三段的四个顶点纵坐标，用于排序获得中枢低点和高点
        pivot_i = 1  # line_i为lineList中元素下标（全局变量）
        startDate = ''  # 中枢开始日期
        endDate = ''  # 中枢结束日期
        pivot_j = 0  # pivot_j为最近一个中枢的元素下标

        buy_list = []
        sell_list = []
        buy_list_history = []
        sell_list_history = []
        update_buy_and_sell = []

        getLine()  # 根据笔端点列表计算线段端点列表
        getPivot(pivot_with_stroke)  # 计算中枢
        getBuyAndSell()  # 计算买卖点

        if JS:
            # 重新组织数据格式，用于前端展示
            pivot = reFormatPivotList()
            # buy, sell = reFormatBuyAndSell(buy_list, sell_list)
            buy_1, buy_2, buy_3, sell_1, sell_2, sell_3 = reFormatBuyAndSell_classifier(buy_list, sell_list)
            # buy_history, sell_history = reFormatBuyAndSell(buy_list_history, sell_list_history)
            # update = reFormatUpdate(update_buy_and_sell)

        # buy_sell_data = {'id':2,'buy_1': buy_1, 'buy_2': buy_2,
        #     'buy_3': buy_3, 'sell_1': sell_1,
        #     'sell_2': sell_2, 'sell_3': sell_3}

        # buy_sell_data = {'id':1, 'buy_1': 'buy_1', 'buy_2': 'buy_2',
        #  'buy_3': 'buy_3', 'sell_1': 'sell_1',
        #  'sell_2': 'sell_2', 'sell_3': 'sell_3'}

        # insertSql = "INSERT INTO entanglement VALUES(?,?,?,?,?,?,?)"
        # Connect.executemany(insertSql, df_buy_sell_data)
        # Connect.commit()
        # df_buy_sell_data.to_sql('entanglement', Connect, if_exists="append", index=True)

    except Exception as e:
        print('Error:', e.args[0])
        if JS:
            Macd = {'dif': [], 'dea': [], 'macd': []}
            Macd = json.dumps(Macd)
            # stroke = []
            stroke_new = []

            stroke_i = 0
            stroke_combine = []
            lineList = []
            stroke_j = 1
            jLen = -1
            up = 0
            last_i = 0
            last_line = []

            pivotList = []  # 中枢列表[[日期1，日期2，中枢低点，中枢高点]]
            currentPivot = []  # 暂存中枢前三段的四个顶点纵坐标，用于排序获得中枢低点和高点
            pivot_i = 1  # line_i为lineList中元素下标（全局变量）
            startDate = ''  # 中枢开始日期
            endDate = ''  # 中枢结束日期
            pivot_j = 0  # pivot_j为最近一个中枢的元素下标
            pivot = "[]"  # pivot列表的Javascript格式字符串
            # buy = "[]"
            # sell = "[]"
            # buy_history = "[]"
            # sell_history = "[]"
            # update = "[]"

    try:
        if JS:
            js = """
                var data = getData(%s["data"])
                var Macd = %s
            myChart.setOption({
                xAxis: [
                    {data:data.date},
                    {data:data.date},
                    {data:data.date}],
                series: [{
                    name: '日线',
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    data: data.values,
                },{
                    name: 'MA10',
                    data: %s["data"],
                },{
                    name: 'MA20',
                    data: %s["data"],
                },{
                    name: 'MA30',
                    data: %s["data"],
                },{
                    name: 'MACD',
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    data: Macd["macd"],
                    itemStyle:{
                        normal:{
                            color:function(params){
                                if(params.value >0){
                                    return color_red;
                                }else{
                                    return color_green;
                                }
                            }
                        }
                    }
                },{
                    name: 'DIF',
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    data: Macd["dif"]
                },{
                    name: 'DEA',
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    data: Macd["dea"]
                },{
                    name: '成交量',
                    xAxisIndex: 2,
                    yAxisIndex: 2,
                    data: data.volumes
                },{
                    name: '旧笔',
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    data: %s
                },
                {
                    name: '一买',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "bottom"
                        }
                    }
                },{
                    name: '二买',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "bottom"
                        }
                    }
                },{
                    name: '三买',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "bottom"
                        }
                    }
                },{
                    name: '一卖',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "top"
                        }
                    }
                },{
                    name: '二卖',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "top"
                        }
                    }
                },{
                    name: '三卖',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markPoint: {
                        data: %s,
                        symbolSize: 20,
                        label: {
                            show: true,
                            position: "top"
                        }
                    }
                }
                ,{
                    name: '线段',
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    data: %s
                },{
                    name: '中枢',
                    data: [],
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    markArea: {
                        data: %s
                    },
                }]
            });
                """ % (
                rawData.to_json(orient='split'), Macd, ma10, ma20, ma30, stroke, buy_1, buy_2, buy_3, sell_1, sell_2,
                sell_3, lineList, pivot)
            return js
        else:  # 选股模块调用时只需返回买卖点列表
            return buy_list, sell_list
    except:
        pass


def get_bi_list(rows, freq, symbol):
    """
    根据K线数据获取笔列表
    """

    # 根据K线数据获取bars
    bars = []
    for i, row in rows.iterrows():
        # row = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume', 'Money']
        dt = pd.to_datetime(row['Date'])

        if int(row['Volume']) > 0:
            bars.append(RawBar(symbol=symbol, dt=dt, id=i, freq=freq_obj_map[freq],
                               open=round(float(row['Open']), 2),
                               close=round(float(row['Close']), 2),
                               high=round(float(row['High']), 2),
                               low=round(float(row['Low']), 2),
                               vol=int(row['Volume'])))
    # if "min" in freq:
    #     bars[-1].dt = bar_end_time(bars[-1].dt, m=int(freq.replace("min", "")))

    # 根据bars获取笔列表
    if freq == '1m':
        bars = get_kline('600809.XSHG', freq='1min', end_date=datetime.now(), count=2000)
    c = CZSC(bars, get_signals=get_default_signals)
    bi_point_list = []  # 笔端点列表
    for bi in c.finished_bis:

        if bi.direction.value == '向上':
            bi_point_list.append([bi.fx_b.dt.strftime("%Y-%m-%d %H:%M:%S"), bi.fx_b.high])
        else:
            bi_point_list.append([bi.fx_b.dt.strftime("%Y-%m-%d %H:%M:%S"), bi.fx_b.low])

    return bi_point_list


def combineK(kList):
    """
    K线合并与旧笔划分。

    :param kList: 合并前K线列表【日期，最高价，最低价】
    :type kList: list
    :return combineList: 合并后的K线列表【日期，最高价，最低价】
    :return strokeList: 笔端点列表【日期，纵坐标（最高价或最低价）】
    :rtype: list
    """
    topParting = 0  # 连接笔的最后一个顶分型
    bottomParting = 0  # 连接笔的最后一个底分型
    strokeList = []  # 笔端点列表
    combineList = [kList[0]]
    j = 0  # j指向combineList最后一根K线
    for i in range(1, len(kList)):  # i指向待判断的未合并K线
        if kList[i][1] > combineList[j][1] and kList[i][2] > combineList[j][2]:  # 非包含上升K线
            if j > 0:
                if combineList[j - 1][1] > combineList[j][1]:  # 底分型
                    if j - topParting > 2 and topParting >= bottomParting:  # 更新笔(底)
                        bottomParting = j
                        strokeList.append([combineList[j][0], combineList[j][2]])
                    elif combineList[j][2] < combineList[bottomParting][
                        2] and bottomParting > topParting:  # 出现连续两个底分型，若后者更低，则更新底
                        bottomParting = j
                        strokeList[-1] = [combineList[j][0], combineList[j][2]]
            combineList.append(kList[i])
            j += 1
        elif kList[i][1] < combineList[j][1] and kList[i][2] < combineList[j][2]:  # 非包含下降K线
            if j > 0:
                if combineList[j - 1][1] < combineList[j][1]:  # 顶分型
                    if j - bottomParting > 2 and bottomParting >= topParting:  # 更新笔（顶）
                        topParting = j
                        strokeList.append(combineList[j][0:2])
                    elif combineList[j][1] > combineList[topParting][
                        1] and topParting > bottomParting:  # 出现连续两个顶分型，若后者更高，则更新顶
                        topParting = j
                        strokeList[-1] = combineList[j][0:2]
            combineList.append(kList[i])
            j += 1
        elif kList[i][1] >= combineList[j][1] and kList[i][2] <= combineList[j][2]:  # 包含，i包i-1
            if j > 0:  # 若合并列表中已有2根及以上K线，则根据前一根K线处理包含
                if combineList[j - 1][1] < combineList[j][1]:  # 向上处理
                    combineList[j][1] = kList[i][1]  # 更新高点
                    combineList[j][0] = kList[i][0]  # 更新时间（只有i包i-1情况需要向后更新时间）
                else:  # 向下处理
                    combineList[j][2] = kList[i][2]  # 更新低点
                    combineList[j][0] = kList[i][0]  # 更新时间
            else:  # 合并列表中只有1根K线,则根据最大与最小关系处理包含
                combineList[j] = kList[i]
        elif kList[i][1] <= combineList[j][1] and kList[i][2] >= combineList[j][2]:  # 包含，i-1包i
            if j > 0:  # 若合并列表中已有2根及以上K线，则根据前一根K线处理包含
                if combineList[j - 1][1] < combineList[j][1]:  # 向上处理
                    combineList[j][2] = kList[i][2]  # 更新高点
                else:  # 向下处理
                    combineList[j][1] = kList[i][1]  # 更新低点
    return combineList, strokeList


def combineKNew(kList):
    """
    K线合并与新笔划分。

    :param kList: 合并前K线列表【日期，最高价，最低价】
    :type kList: list
    :return combineList: 合并后的K线列表【日期，最高价，最低价】
    :return strokeList: 笔端点列表【日期，纵坐标（最高价或最低价）】
    :rtype: list
    """
    topParting = 0  # 连接笔的最后一个顶分型
    topParting_i = 0  # 连接笔的最后一个顶分型(对应合并前K线列表)
    bottomParting = 0  # 连接笔的最后一个底分型
    bottomParting_i = 0  # 连接笔的最后一个底分型(对应合并前K线列表)
    strokeList = []  # 笔端点列表
    combineList = [kList[0]]
    j = 0  # j指向combineList最后一根K线
    for i in range(1, len(kList)):  # i指向待判断的未合并K线
        if kList[i][1] > combineList[j][1] and kList[i][2] > combineList[j][2]:  # 非包含上升K线
            if j > 0:
                if combineList[j - 1][1] > combineList[j][1]:  # 底分型
                    if i - 1 - topParting_i > 3 and topParting >= bottomParting:  # 更新笔(底)
                        bottomParting = j
                        bottomParting_i = i - 1
                        strokeList.append([combineList[j][0], combineList[j][2]])
                    elif combineList[j][2] < combineList[bottomParting][
                        2] and bottomParting > topParting:  # 出现连续两个底分型，若后者更低，则更新底
                        bottomParting = j
                        bottomParting_i = i - 1
                        strokeList[-1] = [combineList[j][0], combineList[j][2]]
            combineList.append(kList[i])
            j += 1
        elif kList[i][1] < combineList[j][1] and kList[i][2] < combineList[j][2]:  # 非包含下降K线
            if j > 0:
                if combineList[j - 1][1] < combineList[j][1]:  # 顶分型
                    if i - 1 - bottomParting_i > 3 and bottomParting >= topParting:  # 更新笔（顶）
                        topParting = j
                        topParting_i = i - 1
                        strokeList.append(combineList[j][0:2])
                    elif combineList[j][1] > combineList[topParting][
                        1] and topParting > bottomParting:  # 出现连续两个顶分型，若后者更高，则更新顶
                        topParting = j
                        topParting_i = i - 1
                        strokeList[-1] = combineList[j][0:2]
            combineList.append(kList[i])
            j += 1
        elif kList[i][1] >= combineList[j][1] and kList[i][2] <= combineList[j][2]:  # 包含，i包i-1
            if j > 0:  # 若合并列表中已有2根及以上K线，则根据前一根K线处理包含
                if combineList[j - 1][1] < combineList[j][1]:  # 向上处理
                    combineList[j][1] = kList[i][1]  # 更新高点
                    combineList[j][0] = kList[i][0]  # 更新时间（只有i包i-1情况需要向后更新时间）
                else:  # 向下处理
                    combineList[j][2] = kList[i][2]  # 更新低点
                    combineList[j][0] = kList[i][0]  # 更新时间
            else:  # 合并列表中只有1根K线,则根据最大与最小关系处理包含
                combineList[j] = kList[i]
        elif kList[i][1] <= combineList[j][1] and kList[i][2] >= combineList[j][2]:  # 包含，i-1包i
            if j > 0:  # 若合并列表中已有2根及以上K线，则根据前一根K线处理包含
                if combineList[j - 1][1] < combineList[j][1]:  # 向上处理
                    combineList[j][2] = kList[i][2]  # 更新高点
                else:  # 向下处理
                    combineList[j][1] = kList[i][1]  # 更新低点
    return combineList, strokeList


def getLine():
    """计算缠论指标线段。"""
    global stroke_i, stroke_j, jLen, strokesList, stroke_combine, lineList, up, last_i, last_line
    # 判断起始点合理性
    strokeLen = len(strokesList)  # strokeList的长度
    if stroke_i == 0 and strokeLen - stroke_i > 3:  # 若从i开始的strokeList中还有超过三个元素，则有构成线段的可能
        if ((strokesList[stroke_i][1] < strokesList[stroke_i + 1][1]) and (
                strokesList[stroke_i + 3][1] < strokesList[stroke_i][1])) or (
                (strokesList[stroke_i][1] > strokesList[stroke_i + 1][1]) and (
                strokesList[stroke_i + 3][1] > strokesList[stroke_i][1])):  # 三笔间无重叠区域，不构成线段，转向下一坐标点判断
            stroke_i += 1
        lineList.append(strokesList[stroke_i])  # i所指元素构成线段的左端点,只有从头开始判断线段（非增量判断）时才加入左端点值

    while stroke_i < strokeLen:
        stroke_combine.append(strokesList[stroke_i])  # i所指元素加入合并列表
        jLen += 1

        # 若从端点开始判断线段，需确定线段开始的笔方向
        if stroke_j == 1 and jLen == 1:  # 线段刚开始判断且已有一笔在combine列表中
            if stroke_combine[1][1] > stroke_combine[0][1]:  # 向上笔
                up = 1
            else:
                up = -1
        if up == 1:  # 向上笔
            if jLen == 3:  # stroke_combine只包括连续三笔时，构成最初的线段
                if stroke_j == 1:
                    lineList.append(stroke_combine[stroke_j + 2])
                    last_i = stroke_i
                elif stroke_combine[stroke_j + 2][1] > stroke_combine[stroke_j][1]:
                    last_line = lineList[-1]
                    lineList[-1] = stroke_combine[stroke_j + 2]  # 更新线段右端点

            if jLen == 4:  # stroke_j后出现两个特征序列
                if stroke_j >= 3 and stroke_combine[stroke_j + 1][1] < stroke_combine[stroke_j - 2][1]:  # 无缺口时
                    if stroke_combine[stroke_j + 2][1] < stroke_combine[stroke_j][1]:  # 无缺口笔破坏
                        last_line = stroke_combine[stroke_j]
                        stroke_i = last_i - 1
                        stroke_combine = []  # 清空合并列表
                        stroke_j = 1
                        jLen = -1
                        up = 0
                    else:  # 无缺口延伸
                        stroke_j += 2  # stroke_j移向下一个特征序列的起始端
                        last_line = lineList[-1]
                        lineList[-1] = stroke_combine[stroke_j]  # 更新线段右端点
                        last_i = stroke_i - 1
                        jLen = 2
                elif stroke_j >= 3 and stroke_combine[stroke_j + 1][1] >= stroke_combine[stroke_j - 2][1]:  # 有缺口时
                    if stroke_combine[stroke_j + 2][1] > stroke_combine[stroke_j][1]:  # 有缺口延伸
                        stroke_j += 2  # stroke_j移向下一个特征序列的起始端
                        last_line = lineList[-1]
                        lineList[-1] = stroke_combine[stroke_j]  # 更新线段右端点
                        last_i = stroke_i - 1
                        jLen = 2
                    else:  # 有缺口反向分型
                        last_line = stroke_combine[stroke_j]
                        stroke_i = last_i - 1
                        stroke_combine = []  # 清空合并列表
                        stroke_j = 1
                        jLen = -1
                        up = 0
                else:
                    stroke_j += 2
                    jLen = 2
        elif up == -1:  # 向下笔
            if jLen == 3:  # stroke_combine只包括连续三笔时，构成最初的线段
                if stroke_j == 1:
                    lineList.append(stroke_combine[stroke_j + 2])
                    last_i = stroke_i
                elif stroke_combine[stroke_j + 2][1] < stroke_combine[stroke_j][1]:
                    last_line = lineList[-1]
                    lineList[-1] = stroke_combine[stroke_j + 2]  # 更新线段右端点

            if jLen == 4:  # stroke_j后出现两个特征序列
                if stroke_j >= 3 and stroke_combine[stroke_j + 1][1] > stroke_combine[stroke_j - 2][1]:  # 无缺口时
                    if stroke_combine[stroke_j + 2][1] > stroke_combine[stroke_j][1]:  # 无缺口笔破坏
                        last_line = stroke_combine[stroke_j]
                        stroke_i = last_i - 1
                        stroke_combine = []  # 清空合并列表
                        stroke_j = 1
                        jLen = -1
                        up = 0
                    else:  # 无缺口延伸
                        stroke_j += 2  # stroke_j移向下一个特征序列的起始端
                        last_line = lineList[-1]
                        lineList[-1] = stroke_combine[stroke_j]  # 更新线段右端点
                        last_i = stroke_i - 1
                        jLen = 2
                elif stroke_j >= 3 and stroke_combine[stroke_j + 1][1] <= stroke_combine[stroke_j - 2][1]:  # 有缺口时
                    if stroke_combine[stroke_j + 2][1] < stroke_combine[stroke_j][1]:  # 有缺口延伸
                        stroke_j += 2  # stroke_j移向下一个特征序列的起始端
                        lineList[-1] = stroke_combine[stroke_j]  # 更新线段右端点
                        last_i = stroke_i - 1
                        jLen = 2
                    else:  # 有缺口反向分型
                        last_line = stroke_combine[stroke_j]
                        stroke_i = last_i - 1
                        stroke_combine = []  # 清空合并列表
                        stroke_j = 1
                        jLen = -1
                        up = 0
                else:
                    stroke_j += 2
                    jLen = 2
        stroke_i += 1


def getPivot(pivot_with_stroke=1):
    """
    计算缠论指标中枢。

    :param pivot_with_stroke: 是否使用笔计算中枢， pivot_with_stroke=1时使用笔计算中枢，否则使用线段计算中枢
    """
    # 是否需要全局变量？（是：可以增量更新中枢，无须重复计算； *否：中枢数据量小，重新计算开销小）
    global pivot_i, pivot_j, pivotList, strokesList, lineList, currentPivot, startDate, endDate
    if pivot_with_stroke:
        calculateList = strokesList
    else:
        calculateList = lineList
    listLen = len(calculateList)
    if not pivotList and listLen - pivot_i > 3:  # 若从i开始的calculateList中还有超过三个元素，则有构成中枢的可能
        if ((calculateList[pivot_i][1] < calculateList[pivot_i + 1][1]) and (
                calculateList[pivot_i + 3][1] < calculateList[pivot_i][1])) or (
                (calculateList[pivot_i][1] > calculateList[pivot_i + 1][1]) and (
                calculateList[pivot_i + 3][1] > calculateList[pivot_i][1])):  # 三笔间无重叠区域，不构成线段，转向下一坐标点判断
            pivot_i += 1
    while pivot_i < listLen:
        if pivot_j == 0:  # 第一段端点，记录起始日期
            startDate = calculateList[pivot_i][0]
            currentPivot.append(calculateList[pivot_i][1])
            pivot_j += 1
        elif pivot_j < 3:  # 前三段
            currentPivot.append(calculateList[pivot_i][1])
            pivot_j += 1
        elif pivot_j == 3:  # 三段构成最小中枢
            if ((calculateList[pivot_i][1] < calculateList[pivot_i - 1][1]) and (
                    calculateList[pivot_i - 3][1] < calculateList[pivot_i][1])) or (
                    (calculateList[pivot_i][1] > calculateList[pivot_i - 1][1]) and (
                    calculateList[pivot_i - 3][1] > calculateList[pivot_i][1])):
                pivot_j -= 1
                startDate = calculateList[pivot_i - 2][0]
                del currentPivot[0]
            else:
                endDate = calculateList[pivot_i][0]
                currentPivot.append(calculateList[pivot_i][1])
                currentPivot.sort()  # 排序得到中枢低点与高点（前三段次低点与次高点）
                pivotList.append([startDate, endDate, currentPivot[1], currentPivot[2]])
                pivot_j += 1
        else:  # 三段以后
            if pivotList[-1][2] <= calculateList[pivot_i][1] <= pivotList[-1][3]:  # 若下一段端点落在中枢内，则中枢延伸
                pivotList[-1][1] = calculateList[pivot_i][0]
            elif ((pivotList[-1][3] < calculateList[pivot_i][1]) and (
                    pivotList[-1][2] > calculateList[pivot_i - 1][1])) or (
                    (pivotList[-1][2] > calculateList[pivot_i][1]) and (
                    pivotList[-1][3] < calculateList[pivot_i - 1][1])):  # 若pivot_i与pivot_i-1所指元素刚好包住中枢值区间，则中枢延伸
                pivotList[-1][1] = calculateList[pivot_i - 1][0]
            elif ((pivotList[-1][3] < calculateList[pivot_i][1]) and (
                    pivotList[-1][3] < calculateList[pivot_i - 1][1])) or (
                    (pivotList[-1][2] > calculateList[pivot_i][1]) and (
                    pivotList[-1][2] > calculateList[pivot_i - 1][1])):  # 若pivot_i与pivot_i-1所指元素都在中枢值区间外且在同侧，则中枢新生
                if pivot_j == 4:
                    pivot_i -= 1
                else:
                    pivot_i -= 2
                pivot_j = 0
                currentPivot = []

        pivot_i += 1


def getBuyAndSell():
    """计算三类买卖点。"""
    global pivotList, strokesList, buy_list, sell_list, buy_list_history, sell_list_history, update_buy_and_sell
    buy_list = []  # 买点列表
    sell_list = []  # 卖点列表
    buy_list_history = []  # 历史买点列表
    sell_list_history = []  # 历史卖点列表
    update_buy_and_sell = []  # 判断更新买卖点关键位置列表
    it_stroke = 0  # 笔序号
    it_pivot = 1  # 从第二个中枢开始判断（两个及以上中枢形成走势）

    flag = 0  # 走势标记位，用于判断是否更新一类买卖点，1表示上涨，2表示下跌，初始为0，跳过该中枢时flag取反，用于判断是否有连续走势

    buy_n = 0  # 用于记录标记了几个买点
    buy_2 = 0  # 用于记录一买与二买间的间隔笔端点
    old_buy = 0  # 记录上一个中枢的买点数

    sell_n = 0  # 用于记录标记了几个卖点
    sell_2 = 0  # 用于记录一卖与二卖间的间隔笔端点
    old_sell = 0  # 记录上一个中枢的卖点数
    while it_stroke < len(strokesList) and it_pivot < len(pivotList):  # 若笔或中枢列表其一遍历完，则停止循环
        # 若笔端点在中枢左侧，则判断下一个笔端点
        if strokesList[it_stroke][0] < pivotList[it_pivot][0]:
            it_stroke += 1
        # 若笔端点恰为中枢左端点，则笔端点跳至其后第三个端点（至少三笔构成中枢）
        elif strokesList[it_stroke][0] == pivotList[it_pivot][0]:
            if buy_n and buy_list[-1][0] != pivotList[it_pivot - 1][1]:  # 若上一个中枢存在买点，则当前中枢端点为三类买点
                buy_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'B3'])
                buy_n += 1
            if sell_n and sell_list[-1][0] != pivotList[it_pivot - 1][1]:  # 若上一个中枢存在卖点，则当前中枢端点为三类卖点
                sell_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'S3'])
                sell_n += 1

            old_buy = buy_n  # 记录上一个中枢的买点数
            buy_n = 0  # 用于记录标记了几个买点
            buy_2 = 0  # 用于记录一买与二买间的间隔笔端点

            old_sell = sell_n  # 记录上一个中枢的卖点数
            sell_n = 0  # 用于记录标记了几个卖点
            sell_2 = 0  # 用于记录一卖与二卖间的间隔笔端点

            it_stroke += 3  # 跳转至形成中枢后的一个笔端点（至少三笔才能构成中枢，也才能继续判断买卖点的产生情况）
        # 若笔端点在中枢内部或右端点，则判断是否为买卖点
        elif strokesList[it_stroke][0] <= pivotList[it_pivot][1]:
            # 卖点计算函数
            if (pivotList[it_pivot][2] + pivotList[it_pivot][3]) > (
                    pivotList[it_pivot - 1][2] + pivotList[it_pivot - 1][3]):  # 中枢上升，上涨走势
                if flag == -1:  # 出现连续上涨走势
                    update_buy_and_sell.append(strokesList[it_stroke])  # 判断更新卖点数据的关机键位置
                    # 删除原来记录的卖点（更新卖点前的操作）
                    t = -1
                    while old_sell:
                        # s = sell_list.pop()
                        s = sell_list[t]
                        t -= 1
                        sell_list_history.append(s)
                        old_sell -= 1
                # 先判断第一类卖点
                if sell_n == 0:
                    if strokesList[it_stroke][1] > pivotList[it_pivot][3]:
                        sell_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'S1'])
                        sell_n += 1
                # 再判断第二类卖点
                else:
                    if sell_2 == 0:  # 跳过第一类卖点的下一个端点才到第二个卖点
                        sell_2 = 1
                    elif sell_2 == 1:
                        sell_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'S2'])
                        sell_n += 1
                        sell_2 = 2
                flag = 1  # 在最后标记为上涨走势
            # 买点计算函数
            else:  # 中枢下降，下跌走势
                if flag == -2:  # 出现连续下跌走势
                    update_buy_and_sell.append(strokesList[it_stroke])  # 判断更新卖点数据的关机键位置
                    # 删除原来记录的买点（更新买点前的操作）
                    t = -1
                    while old_buy:
                        # b = buy_list.pop()
                        b = buy_list[t]
                        t -= 1
                        buy_list_history.append(b)
                        old_buy -= 1
                # 先判断第一类买点
                if buy_n == 0:
                    if strokesList[it_stroke][1] < pivotList[it_pivot][2]:
                        buy_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'B1'])
                        buy_n += 1
                # 再判断第二类买点
                else:
                    if buy_2 == 0:  # 跳过第一类买点的下一个端点才到第二个买点
                        buy_2 = 1
                    elif buy_2 == 1:
                        buy_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'B2'])
                        buy_n += 1
                        buy_2 = 2
                flag = 2  # 在最后标记为下跌走势
            it_stroke += 1
        # 若笔端点超过中枢右端点，则跳至下一中枢判断
        elif strokesList[it_stroke][0] > pivotList[it_pivot][1]:
            it_pivot += 1
            flag *= -1  # 将flag取反，若出现连续走势，则检测到flag=-1时更新卖点，检测到flag=-2时更新买点


def reFormatPivotList():
    """将中枢列表更改为js指定格式字符串。"""
    rePivotList = "["
    for Item in pivotList:
        rePivotList += "[{coord: ['%s', %s]},{coord: ['%s', %s]}]," % (Item[0], Item[2], Item[1], Item[3])
    rePivotList += "]"
    return rePivotList


def reFormatBuyAndSell(buy, sell):
    """将买卖点列表更改为js指定格式字符串。"""
    reBuyList = "["
    if buy:
        for Item in buy:
            reBuyList += "{name:'买点', coord: ['%s', %s], value: %s}," % (Item[0], Item[1], Item[1])
    reBuyList += "]"
    reSellList = "["
    if sell:
        for Item in sell:
            reSellList += "{name:'卖点', coord: ['%s', %s], value: %s}," % (Item[0], Item[1], Item[1])
    reSellList += "]"
    return reBuyList, reSellList


def reFormatBuyAndSell_classifier(buy, sell, freq):
    """将买卖点列表更改为js指定格式字符串。"""
    reBuyList_1 = "["
    reBuyList_2 = "["
    reBuyList_3 = "["

    dataBase = Path(__file__).parent.parent.joinpath("DataBase", "stockData.db")
    Connect = sqlite3.connect(dataBase)

    # if buy:
    #     for Item in buy:
    #         if Item[2] == 'B1':
    #             reBuyList_1 += "{name:'买点', coord: ['%s', %s], value: %s}," % (Item[0], Item[1], Item[1])
    #         elif Item[2] == 'B2':
    #             reBuyList_2 += "{name:'买点', coord: ['%s', %s], value: %s}," % (Item[0], Item[1], Item[1])
    #         elif Item[2] == 'B3':
    #             reBuyList_3 += "{name:'买点', coord: ['%s', %s], value: %s}," % (Item[0], Item[1], Item[1])
    #
    #         buy_data = {'type':Item[2], 'price':Item[1], 'time':Item[0]}
    # buy_data = {'type':'B1', 'price':23.0, 'time':'2021-9-17'}
    # df_buy_data = pd.DataFrame(data=buy_data,index = [1])
    # df_buy_data.to_sql('buy_sell_point_history', Connect, if_exists="append", index=True)

    last_eval_time = getLastTime(freq)

    for record in getPoints('B1', freq, last_eval_time):
        reBuyList_1 += "{name:'买点', coord: ['%s', %s], value: %s}," % (
            record[3].strftime("%Y-%m-%d %H:%M:%S"), record[9], record[9])
    for record in getPoints('B2', freq, last_eval_time):
        reBuyList_2 += "{name:'买点', coord: ['%s', %s], value: %s}," % (
            record[3].strftime("%Y-%m-%d %H:%M:%S"), record[9], record[9])
    for record in getPoints('B3', freq, last_eval_time):
        reBuyList_3 += "{name:'买点', coord: ['%s', %s], value: %s}," % (
            record[3].strftime("%Y-%m-%d %H:%M:%S"), record[9], record[9])
    reBuyList_1 += "]"
    reBuyList_2 += "]"
    reBuyList_3 += "]"

    # print(reBuyList_3)

    reSellList_1 = "["
    reSellList_2 = "["
    reSellList_3 = "["

    for record in getPoints('S1', freq, last_eval_time):
        reSellList_1 += "{name:'卖点', coord: ['%s', %s], value: %s}," % (
            record[3].strftime("%Y-%m-%d %H:%M:%S"), record[8], record[8])
    for record in getPoints('S2', freq, last_eval_time):
        reSellList_2 += "{name:'卖点', coord: ['%s', %s], value: %s}," % (
            record[3].strftime("%Y-%m-%d %H:%M:%S"), record[8], record[8])
    for record in getPoints('S3', freq, last_eval_time):
        reSellList_3 += "{name:'卖点', coord: ['%s', %s], value: %s}," % (
            record[3].strftime("%Y-%m-%d %H:%M:%S"), record[8], record[8])
    # if sell:
    #     for Item in sell:
    #         if Item[2] == 'S1':
    #             reSellList_1 += "{name:'卖点', coord: ['%s', %s], value: %s}," % (Item[0], Item[1], Item[1])
    #         elif Item[2] == 'S2':
    #             reSellList_2 += "{name:'卖点', coord: ['%s', %s], value: %s}," % (Item[0], Item[1], Item[1])
    #         elif Item[2] == 'S3':
    #             reSellList_3 += "{name:'卖点', coord: ['%s', %s], value: %s}," % (Item[0], Item[1], Item[1])
    #
    #         sell_data = {'type':Item[2], 'price':Item[1], 'time':Item[0]}
    # buy_data = {'type':'S1', 'price':23.0, 'time':'2021-9-17'}
    # df_sell_data = pd.DataFrame(data=sell_data,index = [1])
    # df_sell_data.to_sql('buy_sell_point_history', Connect, if_exists="append", index=True)

    reSellList_1 += "]"
    reSellList_2 += "]"
    reSellList_3 += "]"

    Connect.close()
    return reBuyList_1, reBuyList_2, reBuyList_3, reSellList_1, reSellList_2, reSellList_3


def reFormatUpdate(update):
    """将更新买卖点关键位置列表更改为js指定格式字符串。"""
    re_update = "["
    if update:
        for Item in update:
            re_update += "{name:'更新判断', coord: ['%s', %s]}," % (Item[0], Item[1])
    re_update += "]"
    return re_update


def getLastTime(freq):
    cursor = db.cursor()

    last_time = None
    try:
        cursor.execute(
            """select max(evaluation_time) from cl_point_result where stock_id=%s and level=%s""",
            (stock_id, freq_map[freq]))
        last_time = cursor.fetchone()
    except:
        print("查询失败!")

    return last_time


def getPoints(type, freq, last_time):
    cursor = db.cursor()

    # 只获取最新计算的买卖点
    records = None
    # if last_time:
    #     try:
    #         cursor.execute(
    #             """select * from cl_point_result where valid=%s and stock_id=%s and level=%s and type=%s and evaluation_time=%s and id in(select max(id) from cl_point_result group by point)""",
    #             (1, stock_id, freq_map[freq], type, last_time))
    #         records = cursor.fetchall()
    #     except:
    #         print("查询失败!")

    # try:
    #     cursor.execute(
    #         """select * from cl_point_result where valid=%s and stock_id=%s and level=%s and type=%s and id in(select max(id) from cl_point_result group by point)""",
    #         (1, stock_id, freq_map[freq], type))
    #     records = cursor.fetchall()
    # except:
    #     print("查询失败!")

    records = db_con.get_all('select * from cl_point_result where stock_id=\'%s\' and level=\'%s\' and type=\'%s\'' % (
        stock_id, freq_map[freq], type))
    if type == 'B3' and freq == '1d':
        print(records)

    return records
