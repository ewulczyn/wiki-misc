import sys
import urlparse
import MySQLdb
import sys, traceback
from urlparse import urlparse



INSERT_INTERVAL = 10000

param_names = [
                'curr', 
                'next',
                'count',
                ]


def get_conn():
    conn = MySQLdb.connect(
    host='analytics-store.eqiad.wmnet',
    db='staging',
    read_default_file="/etc/mysql/conf.d/analytics-research-client.cnf"
    )
    return conn



def main():
    conn = get_conn()
    cur = conn.cursor()
    query = "insert into mc (" + ", ".join(param_names) + ") values (" +  ", ".join(["%(" + n + ")s" for n in param_names]) + ") ON DUPLICATE KEY UPDATE count = count + 1;"
    

    i = 0

    params_list = []

    for line in sys.stdin:
        i +=1
        try:
            referer, host, path = line.strip().split('|')

            if host == 'en.wikipedia.org' and path != '/w/api.php' and path !='/w/index.php':
                parsed = urlparse(referer)
                curr = parsed.netloc+parsed.path
                params = { 'curr':curr, 'next': host+path, 'count':1}
                params_list.append(params)

        except:
                traceback.print_exc(file=sys.stdout)


        if i%INSERT_INTERVAL == 0:
            print 'inserting'

            try:
                cur.executemany(query, params_list)
                conn.commit()
                params_list = []

            except:
                traceback.print_exc(file=sys.stdout)



      

    
if __name__ == "__main__":
    main()

    


