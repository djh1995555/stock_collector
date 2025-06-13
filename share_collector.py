
import akshare as ak
import tushare as ts
import pandas as pd
from utils import *
import time

class Share:
	def __init__(self):
		None

	def get_all_stock_id(self):
		None

	def get_stock_hist(self, stock_id, period, start_date, end_date, adjust):
		None

	def get_stock_profit_report(self, stock_id):
		return ak.stock_profit_sheet_by_report_em(symbol=stock_id)

	def get_stock_info(self, stock_id):
		return ak.stock_individual_info_em(stock_id)

	def get_all_concept(self):
		return ak.stock_board_concept_name_em()
	
	def get_stock_of_concept(self, concept):
		return ak.stock_board_concept_cons_em(symbol=concept)
	
class TuShare(Share):
	def __init__(self):
		ts.set_token('953159d227a39e9a2d21963d71edd664662b0649092de804f80aca66')
		self.pro = ts.pro_api()
		# self.area_query_df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
		self.area_query_df = self.read_all_stock_id()
		# self.area_query_df = self.area_query_df.head(20)

	def read_all_stock_id(self):
		dtype_dict = {
				'symbol': str
		}
		all_stock_id_filepath = os.path.join(ROOT_DIR,'all_stock_id_tu.csv')
		df = pd.read_csv(all_stock_id_filepath, dtype=dtype_dict)
		return df

	def get_all_stock_id(self):
		df = self.area_query_df.copy()
		df = self.area_query_df.drop(['ts_code','area','industry','list_date'], axis=1)
		df = df.rename(columns={'symbol': '代码', 'name': '名称'})
		return df
	
	def get_stock_hist(self, stock_id, period, start_date, end_date, adjust):
		start_ts = time.time()
		stock_id_with_area = self.query_stock_id_with_area(stock_id)
		df = self.pro.daily(ts_code=stock_id_with_area, adj=adjust, start_date=start_date, end_date=end_date)
		df = self.pose_process(df)
		return df
	
	def query_stock_id_with_area(self, stock_id):
		return self.area_query_df[self.area_query_df['symbol'] == stock_id]['ts_code'].values[0]

	def transform_date(self, row):
		return transform_date(row['日期'])

	def simlify_stock_id(self, row):
		return row['股票代码'].split('.')[0]
	
	def pose_process(self, df):
		if(df.shape[0] == 0):
			return pd.DataFrame()
		df = df.drop(['pre_close'],axis=1)
		df = df.rename(columns={
			'ts_code': '股票代码', 
			'trade_date': '日期',
			'open': '开盘',
			'high': '最高',
			'low': '最低',
			'close': '收盘',
			'change': '涨跌额',
			'pct_chg': '涨跌幅',
			'vol': '成交量',
			'amount': '成交额',
		})
		df['日期'] = df.apply(self.transform_date, axis=1)
		df['股票代码'] = df.apply(self.simlify_stock_id, axis=1)
		df = df[::-1]
		df = df.reset_index()
		return df

class AkShare(Share):
	def __init__(self):
		None

	def get_all_stock_id(self):
		# target_areas = ['shanghai', 'shenzhen','beijing']
		# df = pd.DataFrame()
		# for target_area in target_areas:
		# 	tmp_df = self.get_stocks_by_area(target_area)
		# 	if(target_area == 'shanghai'):
		# 		tmp_df['area'] = ['sh'] * tmp_df.shape[0]
		# 	elif(target_area == 'shenzhen'):
		# 		tmp_df['area'] = ['sz'] * tmp_df.shape[0]
		# 	elif(target_area == 'beijing'):
		# 		tmp_df['area'] = ['bj'] * tmp_df.shape[0]
		# 	df = pd.concat([df, tmp_df])
		dtype_dict = {
				'代码': str
		}
		all_stock_id_filepath = os.path.join(ROOT_DIR,'all_stock_id.csv')
		df = pd.read_csv(all_stock_id_filepath, dtype=dtype_dict)
		print(f'query {df.shape[0]} stocks data!')
		df = df[['代码','名称']]
		return df

	def get_stocks_by_area(self, type):
		print(f'query data of {type}!')
		stock_dict = {
			'all':ak.stock_zh_a_spot_em,
			'shanghai':ak.stock_sh_a_spot_em,
			'shenzhen':ak.stock_sz_a_spot_em,
			'beijing':ak.stock_bj_a_spot_em,
			'chuangyeban':ak.stock_cy_a_spot_em,
			'kechuangban':ak.stock_zh_kcb_spot,
		}
		return stock_dict[type]()

	def get_stock_hist(self, stock_id, period, start_date, end_date, adjust):
		return ak.stock_zh_a_hist(symbol=stock_id, period=period, start_date = start_date, end_date = end_date, adjust=adjust)
	

class ShareCollector:
	def __init__(self, conf):
		self.conf = conf
		if(self.conf['share_type'] == 'TuShare'):
			self.share = TuShare()
		else:
			self.share = AkShare()

	def get_all_stock_id(self):
		return self.share.get_all_stock_id()
	
	def get_stock_hist(self, stock_id, period, start_date, end_date, adjust):
		return self.share.get_stock_hist(stock_id, period, start_date, end_date, adjust)

	def get_stock_profit_report(self, stock_id):
		return self.share.get_stock_profit_report(stock_id)

	def get_stock_info(self, stock_id):
		return self.share.get_stock_info(stock_id)

	def get_all_concept(self):
		return self.share.get_all_concept()
	
	def get_stock_of_concept(self, concept):
		return self.share.get_stock_of_concept(concept)
	
