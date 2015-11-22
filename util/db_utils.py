##### UTILITY FUNCTIONS
import pymysql
import pandas as pd
import os
from datetime import datetime
import dateutil.parser
from dateutil import relativedelta
import traceback
import sys


def query_through_tunnel(port,cnf_path, query, params):
    conn = pymysql.connect(host="127.0.0.1", port=port, read_default_file=cnf_path)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return mysql_to_pandas(rows)


def mysql_to_pandas(dicts):
    dmaster = {}
    for d in dicts:
        for k in d.keys():
            if k not in dmaster:
                dmaster[k] = []

            elem = d[k]
            if isinstance(elem, bytes):
                elem  = elem.decode('utf8')
            dmaster[k].append(elem) 
    return pd.DataFrame(dmaster)


def query_lutetium(query, params):
    #ssh lutetium
    return query_through_tunnel(8000, "~/.lutetium.cnf", query, params)

def query_analytics_store(query, params):
    # ssh bast1001.wikimedia.org
    return query_through_tunnel(8001, "~/.stat3.cnf", query, params)

def query_s1(query, params):
    # ssh bast1001.wikimedia.org
    return query_through_tunnel(8002, "~/.stat3.cnf", query, params)




def query_lutetium_ssh(query, file_name):
    try:
        cmd = """ssh lutetium "mysql  --defaults-file=~/.my.cnf -e \\" """ +query+ """ \\" --socket  /tmp/mysql.sock"> """+ file_name
        os.system(cmd)
        d = pd.read_csv(file_name,  sep='\t')
        os.system('rm ' + file_name)
        return d
    except:
        print(traceback.format_exc())
        os.system('rm ' + file_name)
        print("QUERY FAILED")


def query_lutetium_robust(query, params):
    # if the client does not have mysqldb, this wont work
    try:
        return query_lutetium(query, params)
    # so use ssh to query. This is not thread safe and absolutely rediculous
    except:
        print ("fetching data via ssh")

        query = query % params
        file_name = str(hashlib.md5(query.encode()).hexdigest())
        return query_lutetium_ssh(query, file_name)
    

def query_hive_ssh(query, file_name, priority = False, delete = True):
    if priority:
        query = "SET mapreduce.job.queuename=priority;" + query
    cmd = """ssh stat1002.eqiad.wmnet "hive -e \\" """ +query+ """ \\" "> """ + file_name
    os.system(cmd)
    d = pd.read_csv(file_name,  sep='\t')
    if delete:
        os.system('rm ' + file_name)
    return d


def execute_hive_expression(query, priority = False):
    if priority:
        query = "SET mapreduce.job.queuename=priority;" + query
    cmd = """ssh stat1002.eqiad.wmnet "hive -e \\" """ +query+ """ \\" "> """ + 'hive_exec_dummy'
    os.system(cmd)
    



def query_stat_ssh(query, file_name):
    cmd = """ssh stat1002.eqiad.wmnet "mysql --defaults-file=/etc/mysql/conf.d/analytics-research-client.cnf -h analytics-store.eqiad.wmnet -u research -e \\" """ +query+ """ \\" --socket  /tmp/mysql.sock  "> """+ file_name
    os.system(cmd)
    d = pd.read_csv(file_name,  sep='\t')
    os.system('rm ' + file_name)
    return d



def get_hive_timespan(start, stop, hour = False):
    start = dateutil.parser.parse(start)
    stop = dateutil.parser.parse(stop)
    days = []
    while start <= stop:
        parts = []
        parts.append('year=%d' % start.year)
        parts.append('month=%d' % start.month)
        parts.append('day=%d' % start.day)
        if hour:
            parts.append('hour=%d' % start.hour)
            start += relativedelta.relativedelta(hours=1)
        else:
            start += relativedelta.relativedelta(days=1)
        days.append(' AND '.join(parts))

    
    condition = '((' + (') OR (').join(days) + '))' 
    return condition


def get_time_limits(start = None, stop = None, month_delta = 36):
    """
    Expands start and stop dts into detailed dt parameter dict
    """
    TSFORMAT = '%Y%m%d%H%M%S'
    DATEFORMAT = '%Y-%m-%d %H:%M:%S'
    params  = {}

    if start:
        params['start_dt'] = dateutil.parser.parse(start)
    else:
        # default to 3 month ago
        params['start_dt'] = (datetime.utcnow() - relativedelta.relativedelta(months=month_delta))

    if stop:
        params['stop_dt'] = dateutil.parser.parse(stop)
    else:
        # default to now
        params['stop_dt'] = datetime.utcnow()

    # Timestamp format for queries
    params['start'] = str(params['start_dt'])
    params['stop'] = str(params['stop_dt'])
    params['start_ts'] = params['start_dt'].strftime(TSFORMAT)
    params['stop_ts'] = params['stop_dt'].strftime(TSFORMAT)
    params['start_year'] = params['start_dt'].year
    params['start_month'] = params['start_dt'].month
    params['start_day'] = params['start_dt'].day
    params['start_hour'] = params['start_dt'].hour


    params['stop_year'] = params['stop_dt'].year
    params['stop_month'] = params['stop_dt'].month
    params['stop_day'] = params['stop_dt'].day
    params['stop_hour'] = params['stop_dt'].hour

    return params