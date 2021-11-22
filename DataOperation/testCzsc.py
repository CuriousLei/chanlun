# 首次使用需要设置聚宽账户
from czsc.data.jq import set_token
# set_token("18800166629", '123QWEasdZXC') # 第一个参数是JQData的手机号，第二个参数是登录密码
set_token("18811616732", '123456leidaJQ')
from datetime import datetime
import czsc
from czsc.data.jq import JqCzscTrader as CzscTrader

assert czsc.__version__ == '0.7.9'

# 在默认浏览器中打开最新分析结果，
# ct = CzscTrader(symbol="600809.XSHG", end_date=datetime.now())
ct = CzscTrader(symbol="600807.XSHG", end_date='2017-12-17 09:41:00')
ct.open_in_browser(width="1400px", height="580px")
# open_in_browser 方法可以在windows系统中使用，如果无法使用，可以直接保存结果到 html 文件
# ct.take_snapshot(file_html="czsc_results.html", width="1400px", height="580px")

# 在默认浏览器中打开指定结束日期的分析结果）
# ct = CzscTrader(symbol="000001.XSHG", end_date="2021-03-04")
# ct.open_in_browser(width="1400px", height="580px")
