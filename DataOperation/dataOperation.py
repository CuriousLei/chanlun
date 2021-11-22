"""数据处理模块"""
import json
import os
import logging
import time
from pathlib import Path


ROOT_PATH = Path(__file__).parent.parent


class FileOperation:
    """文件管理模块。"""
    @staticmethod
    def loadJsonFile(filename):
        """
        加载并返回json数据。

        :param filename: 加载json文件名称
        :type filename: pathlib.WindowsPath
        """
        if os.path.isfile(filename):
            with open(filename, 'r', encoding='utf-8') as file:
                file_load = json.load(file)
                return file_load
        else:  # 若不存在此文件，则新建一个空文件
            with open(filename, 'w', encoding='utf-8') as file:
                json.dump([], file)

    @staticmethod
    def saveJsonFile(data, filename='', orient='records', write=1):
        """
        将dataframe数据导出为json格式。

        :param data: 输入的dataframe数据
        :param filename: 输出文件名
        :param orient: 输出json数据格式，默认为records格式
        :param write: 是否写入文件，默认为 write=1 写入文件
        :type data: dataframe
        :type filename: pathlib.WindowsPath
        :type orient: str
        :type write: bool
        :return: json格式数据
        :rtype: json
        """
        file_to_json = data.to_json(orient=orient, force_ascii=False)
        if not write:  # 若不写入文件，则只转换格式
            return file_to_json
        else:
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(file_to_json)

    @staticmethod
    def appendJsonFile(data, filename):
        """
        向json文件中添加数据。

        :param data: 待添加数据
        :param filename: json文件名
        :type data: dict
        :type filename: pathlib.WindowsPath
        """
        with open(filename, 'r', encoding='utf-8') as file:
            file_load = json.load(file)
        with open(filename, 'w', encoding='utf-8') as file:
            file_load.append(data)
            json.dump(file_load, file)

    @staticmethod
    def delJsonFile(data, filename):
        """
        从json文件中删除数据。

        :param data: 待删除数据
        :param filename: json文件名
        :type data: dict
        :type filename: pathlib.WindowsPath
        """
        with open(filename, 'r', encoding='utf-8') as file:
            file_load = json.load(file)
        with open(filename, 'w', encoding='utf-8') as file:
            file_load.remove(data)
            json.dump(file_load, file)

    @staticmethod
    def logOperation(info='', warning=0, existTest=0):
        """
        写日志,也可检测今日日志是否存在。

        :param info: 写入日志内容
        :param warning: warning=1时写入WARNING，反之写入INFO
        :param existTest: existTest=1时测试今日日志是否存在
        :type info: str
        :type warning: bool
        :type existTest: bool
        :return: 返回日志文件是否存在或返回文件路径
        """
        file = ROOT_PATH.joinpath('DataBase', 'Log', time.strftime("%Y%m%d", time.localtime()) + '.log')
        if existTest:
            if not os.path.isfile(file):
                return False
            else:
                return file
        timeHandler = logging.FileHandler(file, encoding="utf-8", mode="a")
        timeFormat = logging.Formatter('%(asctime)s[%(levelname)s]\n%(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S')
        timeHandler.setFormatter(timeFormat)
        timeLogger = logging.getLogger()
        timeLogger.addHandler(timeHandler)
        timeLogger.setLevel(logging.INFO)
        if warning:
            timeLogger.warning(info)
            timeLogger.removeHandler(timeHandler)  # 防止重复写入日志
        else:
            timeLogger.info(info)
            timeLogger.removeHandler(timeHandler)  # 防止重复写入日志

    @staticmethod
    def logLoader():
        """
        读日志，默认读入当天日志内容。

        :return: 返回读取日志内容
        """
        file = ROOT_PATH.joinpath('DataBase', 'Log', time.strftime("%Y%m%d", time.localtime()) + '.log')
        with open(file, 'r', encoding='utf-8') as log:
            info = log.read()
            return info


def formerComplexRights(rawData):
    """
    根据存入数据库中的格式化行情格式(dataAcquisition/get_daily())计算前复权数据(复权数据会由于复权因子的改变而改变，使用时需要重新计算)。

    :param rawData: 未复权数据
    :type rawData: dataframe
    :return: 前复权数据
    :rtype: dataframe
    """
    # todo 比较没有跳空的股票的复权数据，比较复权股票的数据，尝试将复权数据存入数据库
    currentFactor = rawData.iloc[-1:, 6:].to_numpy()[0][0]  # 当天复权因子
    final = rawData.copy(deep=True)  # 复制未复权数据，用于存储前复权数据
    final["Factor"] = final["Factor"].apply(lambda x: x / currentFactor)  # 前复权因子
    final["Open"] = final.apply(lambda x: x["Open"] * x["Factor"], axis=1)
    final["Close"] = final.apply(lambda x: x["Close"] * x["Factor"], axis=1)
    final["Low"] = final.apply(lambda x: x["Low"] * x["Factor"], axis=1)
    final["High"] = final.apply(lambda x: x["High"] * x["Factor"], axis=1)
    final = final.round(3)  # 保留3位小数
    return final


def compareStockList(text1, text2):
    """
    使用归并算法比较股票列表异同，并以字典格式返回差异内容（用于检查股票列表更新情况）。

    :param text1: 旧文本列表
    :param text2: 新文本列表
    :type text1: json
    :type text2: json
    :return addList: 新增数据
    :return delList: 删除数据
    :return difStr: 差异数据（新增与删除数据）
    :rtype addList: dict
    :rtype delList: dict
    :rtype difStr: str
    """
    i = j = 0
    len1 = len(text1)
    len2 = len(text2)
    addList = []  # 新增股票
    delList = []  # 退市股票
    difStr = ''  # 转为字符串记录
    while i < len1 and j < len2:
        if text1[i]['symbol'] == text2[j]['symbol']:
            i += 1
            j += 1
        elif text1[i]['symbol'] < text2[j]['symbol']:
            delList.append(text1[i])
            difStr += 'Del: %s\n' % text1[i]
            i += 1
        elif text1[i]['symbol'] > text2[j]['symbol']:
            addList.append(text2[j])
            difStr += 'Add: %s\n' % text2[j]
            j += 1
    while i < len1:
        delList.append(text1[i])
        difStr += 'Del: %s\n' % text1[i]
        i += 1
    while j < len2:
        addList.append(text2[j])
        difStr += 'Add: %s\n' % text2[j]
        j += 1
    return addList, delList, difStr
