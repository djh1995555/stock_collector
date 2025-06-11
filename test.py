# import akshare as ak

# stock_id = 'sh000045'
# end_date = '20250606'
# start_date = '20250305'
# # ak.stock_zh_a_hist(symbol=stock_id, period="daily", start_date = start_date, end_date = end_date, adjust="qfq")

# ak.stock_zh_a_hist_tx(symbol=stock_id, start_date = start_date, end_date = end_date, adjust="qfq")

# import akshare as ak
# import pandas as pd

# df = pd.read_csv('all_stock_id2.csv')
# for i, row in df.iterrows():
# 	df.loc[i, 'type'] = 1
# df.to_csv('tmp.csv')

import tushare as ts
ts.set_token('953159d227a39e9a2d21963d71edd664662b0649092de804f80aca66')
df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20180101', end_date='20181011')
df.to_csv('tmp.csv')