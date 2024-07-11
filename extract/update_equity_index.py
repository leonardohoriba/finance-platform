from routines import update_index_data
from datetime import datetime


try:
	print(f'update_equity_index.py: {datetime.now().strftime("%H:%M:%S")}')
	update_index_data()

except Exception as e:
	print(e)
