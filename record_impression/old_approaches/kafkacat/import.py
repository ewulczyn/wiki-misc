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


def get_conn():
    conn = MySQLdb.connect(
    host='analytics-store.eqiad.wmnet',
    db='staging',
    read_default_file="/etc/mysql/conf.d/analytics-research-client.cnf"
    )
    return conn




def count(df):
    r = df.ix[df.index[0]]
    r['count'] = df.shape[0]
    return r


def get_minute(t):
    return t[:10]+ " " +t[11:-2] + "00"

def main():
    num_lines = 0
    num_errors = 0
    p = re.compile('^C.*FR$')
    params = []
    for line in sys.stdin:
        num_lines += 1
        try:
            d = json.loads(line)
            query_params =  dict(urlparse.parse_qsl(d['uri_query'][1:])) #remove question mark from first query parameter
            if 'campaign' in query_params and p.match(query_params['campaign']):
                query_params["dt"] = get_minute(d['dt'])
                query_params = query_params.items()
                query_params.sort()
                params.append(tuple(query_params))

        except:
            num_errors += 1
            print line
            traceback.print_exc(file=sys.stdout)

    cols = ['anonymous', 'banner', 'bucket', 'campaign', 'country', 'db', 'device', 'project', 'reason', 'result', 'uselang', 'dt', 'count']
    defualt_d = {c:None for c in cols}


    cntr = collections.Counter(params)
    rows = []
    for k, v in cntr.iteritems():
        d = dict(defualt_d)
        d.update(k)
        d['count'] = v
        rows.append(d)

    

    

    query = "insert into record_impression \
    (anonymous, banner, bucket, campaign, country, db, device, dt, project, reason, result, uselang, count) \
    values ( %(anonymous)s, %(banner)s, %(bucket)s, %(campaign)s, %(country)s, %(db)s, %(device)s, %(dt)s, %(project)s, %(reason)s, %(result)s, %(uselang)s, %(count)s)"
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.executemany(query, rows)
        conn.commit()
        conn.close()
    except:
        traceback.print_exc(file=sys.stdout)
 

    print "Num Lines: %d" % num_lines
    print "Num Errors: %d" % num_errors

    
if __name__ == "__main__":
    main()

    


