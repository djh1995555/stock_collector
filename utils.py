import os
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

def insert_string(original_string, position, inserted_string):
	return original_string[:position] + inserted_string + original_string[position:]

def transform_date(date, to_datetime = True):
	date = insert_string(date,4,'-')
	date = insert_string(date,7,'-')
	if(to_datetime):
		date = datetime.strptime(date, "%Y-%m-%d").date()
	return date

def round_num(num, decimal_num):
	number = Decimal(num)
	formate = '0.'
	for i in range(decimal_num):
		formate += '0'
	return number.quantize(Decimal(formate), rounding=ROUND_HALF_UP)