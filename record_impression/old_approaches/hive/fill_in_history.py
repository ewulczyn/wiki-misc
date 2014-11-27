

from import_impressions import import_hour
from datetime import datetime, timedelta

num_hours = 28*24
curr_time = datetime.utcnow()
dts = [curr_time - timedelta(hours = x+3) for x in range(num_hours)]

for dt in dts:
	import_hour(dt)