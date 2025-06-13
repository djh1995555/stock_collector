import argparse
import os
import yaml
from utils import *
from stock_selector import StockSelector


def main(args):
	with open(args.config, 'r') as f:
			config = yaml.load(f, Loader=yaml.Loader)
	stock_selector = StockSelector(config)
	
	dates = ['20250613']
	for date in dates:
		stock_selector.select_stock(date)

if __name__ == '__main__':
	parser = argparse.ArgumentParser('Stock Selector')
	
	parser.add_argument('--config', default=os.path.join(ROOT_DIR,'config/config.yaml'), type=str)
	args = parser.parse_args()
	main(args)

      
