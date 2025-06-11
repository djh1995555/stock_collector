from datetime import datetime, timedelta
import time
import os
import requests
import pandas as pd
import akshare as ak
from tqdm import tqdm
from env import *

class StockSelector:
	def __init__(self, cfg):
		self.cfg = cfg
		dtype_dict = {
				'代码': str
		}

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
		# self.all_stock_id = df

		# self.all_stock_id = self.get_stocks_by_area('all')

		all_stock_id_filepath = os.path.join(ROOT_DIR,'all_stock_id.csv')
		self.all_stock_id = pd.read_csv(all_stock_id_filepath, dtype=dtype_dict)
		print(f'query {self.all_stock_id.shape[0]} stocks data!')

		self.result_dir = os.path.join(ROOT_DIR, cfg['result_dir'])
		if(not os.path.exists(self.result_dir)):
			os.makedirs(self.result_dir)

		self.stock_data_daily_raw_dir = os.path.join(ROOT_DIR, cfg['stock_data_daily_raw_dir'])
		if(not os.path.exists(self.stock_data_daily_raw_dir)):
			os.makedirs(self.stock_data_daily_raw_dir)

		finance_data_dir = os.path.join(ROOT_DIR, cfg['database_dir'], cfg['finance_data_info']['dir'])
		if(cfg['finance_data_info']['update'] or not os.path.exists(finance_data_dir)):
			self.update_finance_data(finance_data_dir)

		listed_date_filepath = os.path.join(ROOT_DIR, cfg['database_dir'], cfg['listed_date_info']['filepath'])
		if(cfg['listed_date_info']['update'] or not os.path.exists(listed_date_filepath)):
			self.listed_date = self.update_listed_date(listed_date_filepath)
		self.listed_date = pd.read_csv(listed_date_filepath, dtype=dtype_dict)

		all_concepts_filepath = os.path.join(ROOT_DIR, cfg['database_dir'], cfg['concept_info']['all_concepts']['filepath'])
		stocks_of_concept_dir = os.path.join(ROOT_DIR, cfg['database_dir'], cfg['concept_info']['all_concepts']['stocks_of_concept_dir'])
		if(cfg['concept_info']['all_concepts']['update'] or not os.path.exists(all_concepts_filepath)):
			self.update_all_concepts(all_concepts_filepath, stocks_of_concept_dir)
		self.all_concepts = pd.read_csv(all_concepts_filepath, dtype=dtype_dict)
		self.stock_by_concepts = self.read_stock_by_concepts(stocks_of_concept_dir)

		concepts_of_stock_filepath = os.path.join(ROOT_DIR, cfg['database_dir'], cfg['concept_info']['concepts_of_stock']['filepath'])
		if(cfg['concept_info']['concepts_of_stock']['update'] or not os.path.exists(concepts_of_stock_filepath)):
			self.concepts_of_stock = self.update_concepts_of_stock(concepts_of_stock_filepath)
		self.concepts_of_stock = pd.read_csv(concepts_of_stock_filepath, dtype=dtype_dict)

		target_concepts_keywords = cfg['target_concepts_keywords']
		self.target_concepts = self.query_target_concetps(target_concepts_keywords)

	def select_stock(self, date):
		daily_result_dir = f'{self.result_dir}/{date}'
		if(not os.path.exists(daily_result_dir)):
			os.makedirs(daily_result_dir)
		raw_data_filepath = f'{daily_result_dir}/result_{date}_raw.csv'
		df = self.all_stock_id[['代码','名称','市盈率-动态','市净率','总市值','流通市值']]
		if(self.cfg['reload']):
			for i, row in tqdm(df.iterrows(), total = df.shape[0]):
				stock_id = row['代码']
				days_num = self.cfg['days_num']
				data = self.get_data(stock_id, date, days_num)
				if(data.shape[0] == 0):
					continue
				
				data = self.compute_average(data, stock_id, date, self.cfg['average_params'])
				data = self.compute_limit_up_flag(data, stock_id, date)

				week_nums = self.cfg['week_nums']
				week_volumes, df.loc[i,'is_week_volume_increase'] = self.compute_week_volume(data, week_nums, self.cfg['week_volume_increase_factor'])
				for j in range(week_nums):
					df.loc[i,f'{j - week_nums + 1} week volume'] = week_volumes[j]

				df.loc[i,'is_bullish_alignment'] = self.is_bullish_alignment(data, self.cfg['average_params'])
				df.loc[i,'is_hold_after_limit_up'] = self.is_hold_after_limit_up(data, self.cfg['search_days_num'])
			df.to_csv(raw_data_filepath)
					

	def is_hold_after_limit_up(self, df, search_days_num):
		length = df.shape[0]
		if(length < search_days_num):
			return False
		find_limit_up = False
		for i in reversed(range(length - search_days_num, length - 2)):
			row = df.iloc[i,:]
			if(row['is_limit_up']):
				find_limit_up = True
				break
		limit_up_index = i

		if(not find_limit_up):
			return False
		
		if(df.loc[limit_up_index, '涨跌幅'] < -2.0):
			return False
		
		limit_up_row = df.iloc[limit_up_index,:]
		for i in range(limit_up_index + 1, length):
			row = df.iloc[i,:]
			if(row['收盘'] < row['5 average'] or row['成交量'] > 1.5 * limit_up_row['成交量']):
				return False
		return True
	
	def compute_week_volume(self, df, week_nums, week_volume_increase_factor):
		if(df.shape[0] == 0):
			return [None] * week_nums, False
		week_volumes = []
		first_week_volume = 0.0
		re = True
		for i in range(week_nums):
			tmp_df = df.iloc[-5 * i - 5 : -5 * i, : ]
			week_volume = tmp_df.loc[:,'成交量'].sum()
			week_volumes.append(week_volume)
			if(i == 0):
				first_week_volume = week_volume
			else:
				re = re and (week_volume > week_volume_increase_factor * first_week_volume)
		return week_volumes, re
	
	def is_bullish_alignment(self, df, average_params):
		if(df.shape[0] == 0):
			return False
		re = True
		df = df.tail(1)
		average_data = []
		for days_num in average_params:
			average_data.append(df[f'{days_num} average'].values[0])

		for i in range(len(average_data)-1):
			if(average_data[i] is None):
				re = False
				break					
			if(average_data[i] <= average_data[i+1]):
				re = False
				break
		return re
	
	def compute_limit_up_flag(self, df, stock_id, date):
		if(df.shape[0] == 0):
			return pd.DataFrame()

		for i, row in df.iterrows():
			if(i == 0):
				df.loc[i,'is_limit_up'] = None
				continue
			last_row = df.iloc[i-1,:]
			df.loc[i,'is_limit_up'] = self.is_limit_up(row['股票代码'], row['收盘'], last_row['收盘'])

		# filepath = os.path.join(self.stock_data_daily_raw_dir, date, f'{stock_id}_2.csv')
		# df.to_csv(filepath)
		return df

	def is_limit_up(self, stock_id, price_today, price_yesterday):
		price_limit = 0.1
		stock_id = str(stock_id).strip()
		prefix = stock_id[:3]
		if prefix.startswith(('60', '00')):
			price_limit = 0.1
		elif prefix.startswith('30'):
			price_limit = 0.2
		elif prefix.startswith('688'):
			price_limit = 0.2

		limit_up_price = round(price_yesterday * (1 + price_limit), 2)
		return price_today ==limit_up_price
		
	def compute_average(self, df, stock_id, date, average_params):
		if(df.shape[0] == 0):
			return pd.DataFrame()

		end_date = date
		end_data = df[df['日期'] == self.transform_date(end_date)]
		if(end_data.empty):
			return pd.DataFrame()			

		for average_param in average_params:
			df[f'{average_param} average'] = round(df['收盘'].rolling(average_param).mean(),2)
			df[f'error with {average_param} avg'] = round((df['收盘'] - df[f'{average_param} average']) / df['收盘'] * 100,2)

		# filepath = os.path.join(self.stock_data_daily_raw_dir, date, f'{stock_id}_averaged.csv')
		# df.to_csv(filepath)
		return df

	def get_data(self, stock_id, date, days_num):
		target_dir = os.path.join(self.stock_data_daily_raw_dir, date)
		filepath = os.path.join(target_dir, f'{stock_id}.csv')
		if(os.path.exists(filepath)):
			dtype_dict = {
					'股票代码': str
			}
			df = pd.read_csv(filepath, parse_dates=['日期'],dtype=dtype_dict)
			df['日期'] = df['日期'].dt.date	
		else:
			if(not os.path.exists(target_dir)):
				os.makedirs(target_dir)
			end_date = date
			start_date = self.get_target_date(end_date, days_num)
			try:
					time.sleep(0.1)
					df = ak.stock_zh_a_hist(symbol=stock_id, period="daily", start_date = start_date, end_date = end_date, adjust="qfq")
					df.to_csv(filepath)
			except requests.exceptions.ConnectionError:
					print(f'data is missing:{date}_{stock_id}')
					return pd.DataFrame()
		return df

	def insert_string(self, original_string, position, inserted_string):
		return original_string[:position] + inserted_string + original_string[position:]
	
	def transform_date(self, date, to_datetime = True):
		date = self.insert_string(date,4,'-')
		date = self.insert_string(date,7,'-')
		if(to_datetime):
			date = datetime.strptime(date, "%Y-%m-%d").date()
		return date
	
	def get_target_date(self, cur_date, delta_days):
		original_date = datetime.strptime(cur_date, "%Y%m%d")
		target_date = original_date - timedelta(days=delta_days)
		return target_date.strftime("%Y%m%d")
	
	# 输入概念的关键字，找到所有相关的概念
	def query_target_concetps(self, target_concepts_keywords):
		concept_list = []
		for i, row in self.all_concepts.iterrows():
			concept_list.append(row['板块名称'])

		all_target_concepts = []
		for target_concept in target_concepts_keywords:
			for concept in concept_list:
				if(target_concept in concept):
					all_target_concepts.append(concept)
		print(all_target_concepts)
		return all_target_concepts
	
	def update_finance_data(self, finance_data_dir):
			if(not os.path.exists(finance_data_dir)):
				os.makedirs(finance_data_dir)

			df = self.all_stock_id.copy()
			for i, row in df.iterrows():
				stock_id = f"{row['area']}{row['代码']}"
				finace_data_filepath = f"{finance_data_dir}/{row['代码']}.csv"
				try:
						df = ak.stock_profit_sheet_by_report_em(symbol=stock_id)
				except KeyError:
						print(f'data is missing:{stock_id}')
				df.to_csv(finace_data_filepath)

	def get_listed_date(self,row):
		info = ak.stock_individual_info_em(symbol=row['代码'])
		return info.loc[info['item']=='上市时间']['value'].values[0]

	def update_listed_date(self,listed_date_filepath):
		print('update_listed_date')
		df = self.all_stock_id.copy()
		tqdm.pandas()
		df['listed_date'] = df.progress_apply(self.get_listed_date, axis=1)
		df = df[['代码','名称','listed_date']]
		df.to_csv(listed_date_filepath)

	def read_stock_by_concepts(self, stocks_of_concept_dir):
		stock_by_concepts = {}
		for i, row in self.all_concepts.iterrows():
			concept_name = row['板块名称']
			concept_name_valid = concept_name.replace('/','-')
			target_concept_data_filename = f'{stocks_of_concept_dir}/{concept_name_valid}.csv'
			stocks_df = pd.read_csv(target_concept_data_filename)
			stock_by_concepts[concept_name] = stocks_df
		return stock_by_concepts
	
	def update_all_concepts(self, all_concepts_filepath, stocks_of_concept_dir):
		print('update_all_concepts')
		# 更新当前所有的概念
		concept_df = ak.stock_board_concept_name_em()
		concept_df.to_csv(all_concepts_filepath)
		# concept_df = pd.read_csv('concept.csv')

		# 遍历概念获取对应的股票，保存为csv，并存在dict里；如果已经存在csv，就会直接读取
		if(not os.path.exists(stocks_of_concept_dir)):
				os.makedirs(stocks_of_concept_dir)
		
		for i, row in concept_df.iterrows():
			concept_name = row['板块名称']
			concept_name_valid = concept_name.replace('/','-')
			target_concept_data_filename = f'{stocks_of_concept_dir}/{concept_name_valid}.csv'
			stocks_df = ak.stock_board_concept_cons_em(symbol=concept_name)
			stocks_df.to_csv(target_concept_data_filename)

	
	# 查询股票所属概念
	def query_concept_of_stock(self, row):
		stock_id = row['代码']
		concepts = ''
		for key, data in self.stock_by_concepts.items():
			if((data['代码'] == stock_id).any()):
				concepts += f"{key},"
		return concepts

	# 查询所有股票的概念
	def update_concepts_of_stock(self, concepts_of_stock_filepath):
		print('update_concepts_of_stock')
		df = self.all_stock_id.copy()
		tqdm.pandas()
		df['concepts'] = df.progress_apply(self.query_concept_of_stock, axis=1)
		df = df[['代码','名称','concepts']]
		df.to_csv(concepts_of_stock_filepath)

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
	


		




