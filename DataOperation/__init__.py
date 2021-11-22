from .dataAcquisition import *
from .dataOperation import *

def tsData(Token='', progress=None, text=None):
    """
    初始化数据源。

    :param Token: tushare数据接口密钥
    :param progress: 传递高阶进度处理函数 示例: progress=self.setProgress
    :param text: 传递高阶文本展示函数 示例: text=self.setText
    :type progress: function
    :type text: function
    """
    # 先查log文件，若今日的日志不存在，则更新股票列表并记录log，否则不需要再检查更新
    Data = DataSource(Token)  # 初始化数据源实例
    STOCK_LIST = Path(__file__).parent.parent.joinpath("DataBase", "stockList.json")  # 股票列表路径
    DATABASE = Path(__file__).parent.parent.joinpath("DataBase", "stockData.db")  # 数据库路径
    connect = sqlite3.connect(DATABASE)
    text('正在检查今日日志……')
    progress(2)
    if not FileOperation.logOperation(existTest=1):  # 今日日志不存在（即今日未登录系统）
        text("日志不存在。\n")
        progress(4)
        time.sleep(0.3)
        text("正在检查股票列表……")
        progress(6)
        time.sleep(0.3)
        try:
            stockList = Data.getStockList()  # 获取今日股票列表
        except:
            text("无法获取数据，请检查网络连接或稍后重试。")
            time.sleep(2)

        if not os.path.isfile(STOCK_LIST):  # 若股票列表不存在,说明是初次使用系统，则新建json文件并下载所有历史数据
            FileOperation.saveJsonFile(stockList, STOCK_LIST)
            text("已获取股票列表，准备下载所有数据……\n")
            progress(15)
            time.sleep(0.5)
            FileOperation.logOperation('已获取股票列表。')
            GetDataWithThread(progress=progress, text=text).startThread()  # 多线程获取所有数据
        else:  # 股票列表存在时，比较内容是否更新，若更新则更新数据库数据
            stockList_new = json.loads(FileOperation.saveJsonFile(stockList, write=0))  # 新列表
            with open(STOCK_LIST, 'r', encoding='utf-8') as file:
                stockList_old = json.load(file)  # 旧列表
            text("股票列表已存在，正在比对新旧列表……")
            progress(8)
            time.sleep(0.3)
            addList, delList, dif = compareStockList(stockList_old, stockList_new)
            if not dif:  # 新旧列表相同
                text("当前股票列表已是最新。\n")
                progress(10)
                time.sleep(0.3)
                FileOperation.logOperation('当前股票列表已是最新。')
            else:  # 新旧列表不同
                log = '已更新股票列表。\n'
                time.sleep(0.5)
                text(log+dif)
                progress(10)
                FileOperation.logOperation(log + dif)  # 写入日志
                FileOperation.saveJsonFile(stockList, STOCK_LIST)  # 覆盖原来的列表
                text("连接至数据库stockData.db……\n")
                progress(12)
                time.sleep(0.3)

                # 删除退市股票数据
                if delList:
                    text("正在删除退市股票数据……")
                    time.sleep(0.3)
                    for i in delList:
                        text(i)
                        connect.cursor().execute('DROP TABLE "%s";' % i['symbol'])
                    connect.commit()
                    connect.close()
                    text("已删除退市股票数据。\n")
                    progress(15)
                    time.sleep(0.3)
                # 可以在初次使用系统时下载所有数据，在以后打开系统时只检查股票列表是否更新，若更新则下载跟新的数据，其它数据在打开系统后再后台加载
                # 方案一：只更新新增股票数据，其它股票数据可在用户查看股票数据时再更新
                # text("准备更新股票数据：\n")
                # time.sleep(0.5)
                # get_data_with_thread(progress=progress, text=text).start_thread(List=addList)  # 多线程获取所有数据

            # 方案二：更新所有股票数据（启动系统前用户需等待较长时间）
            lastDate = connect.cursor().execute('select Date from "000001" order by Date desc limit 1;').fetchone()[0][:10]  # 获取数据库最近数据日期
            timestamp = time.mktime(time.strptime(lastDate, "%Y-%m-%d"))  # 更改时间格式
            lastDate = time.strftime('%Y%m%d', time.localtime(timestamp))  # 数据库最近数据日期
            newDate = DataSource().getTradeDate(last=1)  # 交易日最新日期
            text("本地数据最近日期为：" + lastDate)
            text("交易日最新日期为：" + newDate)
            if lastDate < newDate:  # 若数据滞后，则更新所有数据
                text("准备更新股票数据：\n")
                time.sleep(0.3)
                lastDate = time.strftime('%Y%m%d', time.localtime(timestamp+86400))  # 将数据库最近数据日期加一天，便于获取新数据
                if not DataSource().getDaily('000001', save=0, start=lastDate, end='').empty:  # 先测试服务器是否更新数据，若更新，则多线程获取数据
                    GetDataWithThread(progress=progress, text=text).startThread(start=lastDate)  # 多线程获取所有数据
                else:
                    text("Tushare服务器未更新今日数据。\n")
                    time.sleep(0.3)
            else:
                text("股票数据已是最新。\n")
    else:
        text("日志已创建。\n")
        text("正在检查数据是否更新……\n")
        # 方案二：更新所有股票数据（启动系统前用户需等待较长时间）
        lastDate = connect.cursor().execute('select Date from "000001" order by Date desc limit 1;').fetchone()[0][:10]  # 获取数据库最近数据日期
        timestamp = time.mktime(time.strptime(lastDate, "%Y-%m-%d"))  # 更改时间格式
        lastDate = time.strftime('%Y%m%d', time.localtime(timestamp))  # 数据库最近数据日期
        newDate = DataSource().getTradeDate(last=1)  # 交易日最新日期
        text("本地数据最近日期为：" + lastDate)
        text("交易日最新日期为：" + newDate)
        if lastDate < newDate:  # 若数据滞后，则更新所有数据
            # text("准备更新股票数据：\n")
            # time.sleep(0.3)
            # lastDate = time.strftime('%Y%m%d', time.localtime(timestamp+86400))  # 将数据库最近数据日期加一天，便于获取新数据
            # if not DataSource().getDaily('000001', save=0, start=lastDate, end='').empty:  # 先测试服务器是否更新数据，若更新，则多线程获取数据
            #     GetDataWithThread(progress=progress, text=text).startThread(start=lastDate)  # 多线程获取所有数据
            # else:
            #     text("Tushare服务器未更新今日数据。\n")
            #     time.sleep(0.3)
            pass  # 展示时不更新数据
        else:
            text("股票数据已是最新。\n")
    text("初始化完成，系统启动中……")
    progress(100)
