import pymysql
import time
db = pymysql.Connect(
    host='127.0.0.1',
    port=3307,
    user='root',
    passwd='123456',
    db='chanlun',
    charset='utf8'
)
stock_id = '689009.XSHG'
point = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
point_type = '1B'
level = '15m'
now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
high = 86.7
low = 77.6
record = None
cursor = db.cursor()

cursor.execute("""insert into cl_point_result (stock_id, point, type, level, evaluation_time, high, low) values(%s, %s, %s, %s, %s, %s, %s)""",
                                (stock_id, point, point_type, level, now, high, low))
db.commit()

cursor.execute("""select * from cl_point_result where stock_id=%s and level=%s and point=%s""", (stock_id, level, point))
record = cursor.fetchone()
print(record)
# try:
#     db.cursor().execute("""select * from cl_point_result where stock_id=%s and level=%s and point=%s""", (stock_id, level, point))
#     record = db.cursor().fetchone()
# except:
#     print("查询失败!")