import sys
import json
import urlparse
import pandas as pd
from pandas.io import sql
import MySQLdb
import numpy as np
import collections
import re
import sys, traceback
import datetime
import os
from datetime import datetime, timedelta

import inspect, os
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
os.sys.path.insert(0,parentdir) 
from record_impression_helpers import  get_params, insert_to_db


def get_hour_from_hive(dt, filename):
    hive_query_prototype = """
    CREATE TEMPORARY FUNCTION ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
    select count(*) as count, a.uri_query, a.minute, a.spider
    from (
        select uri_query, substr(dt, 0, 16) as minute, ua(user_agent)['device_family']= 'Spider' as spider
        from wmf_raw.webrequest
        where uri_path = '/wiki/Special:RecordImpression'
        and year = %(year)d
        and month = %(month)d
        and day = %(day)d
        and hour = %(hour)d
    ) a
    group by a.uri_query, a.minute, a.spider;
    """
    
    hive_query_params = {'year': dt.year, 'month': dt.month, 'day':dt.day, 'hour':dt.hour }
    hive_query = hive_query_prototype % hive_query_params
    print hive_query
    cmd = """hive -S -e \" """ +hive_query+ """ \" > """ + filename
    os.system(cmd)


def import_hour(dt):
    
    filename = str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)+'T'+str(dt.hour)
    get_hour_from_hive(dt, filename)
    
    iterator = open(filename)
    iterator.readline() #tsv header
    def transform(line):
        count, query_params, minute, spider = line.strip().split('\t')
        params = get_params(minute+":00", query_params)
        params['count'] = count #here the data is aggregated already
        params['spider'] = (spider == 'true')
        return params

    insert_to_db('impressions_hive', iterator, transform)
    os.system('rm '+filename)



if __name__ == '__main__':
    if os.path.isfile('/home/ellery/record_impression/hive/.lock'):
        exit() 
    os.system('touch /home/ellery/record_impression/hive/.lock')
    dt = datetime.utcnow() - timedelta(hours = 3)
    import_hour(dt)
    os.system('rm /home/ellery/record_impression/hive/.lock')
