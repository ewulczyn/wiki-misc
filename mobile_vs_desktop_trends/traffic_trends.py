import pandas as pd
import MySQLdb
from pprint import pprint
import datetime
import matplotlib.dates as mdates

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def get_conn():
    conn = MySQLdb.connect(
    host='analytics-store.eqiad.wmnet',
    db='staging',
    read_default_file="~/.my.cnf"
    )
    return conn

def get_cursor(conn, type='dict'):
    if type == 'dict':
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    elif type == 'norm':
        cursor = conn.cursor()
    return cursor

def mysql_to_pandas(dicts):
    dmaster = {}
    for d in dicts:
        for k in d.keys():
            if k not in dmaster:
                dmaster[k] = []
            
            dmaster[k].append(d[k]) 
    return pd.DataFrame(dmaster)



def plot_total_traffic(country):
    query = "select sum(pageviews) as pageviews, min(timestamp) as t\
    from staging.referer_data\
    where country = %s\
    and spider = 0\
    and mobile = %s\
    group by MONTH(timestamp), year(timestamp) order by t;"

    plot_traffic_trends(country, query, "_anywiki_trends")

def plot_US_wikipedia_traffic(country):
    query = "select sum(pageviews) as pageviews, min(timestamp) as t\
    from staging.referer_data\
    where country = %s\
    and wiki = 'en'\
    and spider = 0\
    and mobile = %s\
    group by MONTH(timestamp), year(timestamp) order by t;"

    plot_traffic_trends(country, query, "_enwiki_trends")




def plot_traffic_trends(country, query, extension):

    cursor.execute(query, (country, 1 ))
    mobile_rows = cursor.fetchall()[1:-1]
    t = [datetime.datetime.strptime(d['t'], '%Y-%m-%d %H:%M:%S') for d in mobile_rows]
    mobile = [d['pageviews'] for d in mobile_rows]
 

    cursor.execute(query, (country, 0 ))
    desktop_rows= cursor.fetchall()[1:-1]
    t = [datetime.datetime.strptime(d['t'], '%Y-%m-%d %H:%M:%S') for d in desktop_rows]
    desktop = [d['pageviews'] for d in desktop_rows]
 

    #d = pd.merge(mysql_to_pandas(mobile_rows),mysql_to_pandas(desktop_rows), on = ['t', ]  )
    #d.to_csv(country + "trends"+".csv")
    plt.figure()
    plt.plot(t,mobile, label = 'mobile' )
    plt.plot(t,desktop, label = 'desktop' )
    plt.legend()
    plt.xlabel('time')
    plt.ylabel('pageviews')
    plt.gcf().autofmt_xdate()
    plt.savefig(country + extension+".eps")



def get_traffic_trends_csv(country):
    pass

if __name__ == '__main__':

    conn = get_conn()
    cursor = get_cursor(conn)
    countries = ['US', 'FR', 'IT', 'NL', 'JP', 'CA', 'AU', 'AT', 'DE', 'FR']
    for country in countries:
        print country
        #plot_traffic_trends(country)
        plot_US_wikipedia_traffic(country)

