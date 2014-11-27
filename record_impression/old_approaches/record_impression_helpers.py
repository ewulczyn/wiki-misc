import MySQLdb
import json
import urlparse
import inspect, os
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
os.sys.path.insert(0,parentdir) 
from  ua_parser.user_agent_parser import ParseDevice
import sys,traceback

def get_conn():
    conn = MySQLdb.connect(
    host='analytics-store.eqiad.wmnet',
    db='staging',
    read_default_file="/etc/mysql/conf.d/analytics-research-client.cnf"
    )
    return conn


def get_minute(t):
    return t[:10]+ " " +t[11:-2] + "00"


INSERT_INTERVAL = 10000

param_names = [
                'anonymous',
                'banner',
                'bucket',
                'campaign',
                'country',
                'db',
                'device',
                'project',
                'reason',
                'result',
                'uselang',
                'spider',
                'count',
                'dt'
                ]

def get_insert_query(table):
    insert_query = "insert into %s (" % table
    insert_query += ", ".join(param_names) + ") values (" +  ", ".join(["%(" + n + ")s" for n in param_names]) + ") ON DUPLICATE KEY UPDATE count = count + VALUES(count);"
    return insert_query


defualt_d = {c:'None' for c in param_names}
defualt_d['count'] = 1
defualt_d['bucket'] = -1
defualt_d['spider'] = 0


def get_params(dt, query_params, user_agent = None):
    params = defualt_d.copy()
    params.update(dict(urlparse.parse_qsl(query_params[1:].strip())))
    params["dt"] = get_minute(dt)

    if params['bucket'] == 'null':
            params['bucket'] = -1

    if user_agent:
        device_family = ParseDevice(user_agent)['family']
        if device_family == 'Spider' or device_family == 'Bot':
            params['spider'] = 1
    return params



#transform takes an element from the iterator and returns a param dict
def insert_to_db(table, iterator, transform):
    conn = get_conn()
    cur = conn.cursor()
    insert_query = get_insert_query(table)
    i = 0
    params_list = [] 
    
    for iter_elem in iterator:
        i +=1
        params = transform(iter_elem)
        params_list.append(params)

        if i%INSERT_INTERVAL == 0:
            try:
                print "Insertion"
                cur.executemany(insert_query, params_list)
                conn.commit()
                params_list = []

            except:
                traceback.print_exc(file=sys.stdout)
    conn.close()


