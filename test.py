import akshare as ak
import tushare as ts
import tushare as ts
import time
from utils import *
# ts.set_token('953159d227a39e9a2d21963d71edd664662b0649092de804f80aca66')

# stock_id = '000030'
# end_date = '20250609'
# start_date = '20241211'
# adj = 'qfq'
# # df = ak.stock_zh_a_hist(symbol=stock_id, period="daily", start_date = start_date, end_date = end_date, adjust=adj)
# # df.to_csv('ak.csv')

# pro = ts.pro_api()
# start_ts = time.time()
# for i in range(500):
# 	df = pro.daily(ts_code=f'{stock_id}.SZ', adj=adj, start_date=start_date, end_date=end_date)
# 	# df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
# 	# df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
# 	df.to_csv('tu.csv')
# print(time.time() - start_ts)

print(round_num(9.62555,4))