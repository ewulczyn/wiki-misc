##### UTILITY FUNCTIONS
import MySQLdb
import pandas as pd
import os
def get_lutetium_conn():
    conn = MySQLdb.connect(host="127.0.0.1", port=8000, read_default_file="~/.lutetium.cnf")
    return conn

def mysql_to_pandas(dicts):
    dmaster = {}
    for d in dicts:
        for k in d.keys():
            if k not in dmaster:
                dmaster[k] = []
            
            dmaster[k].append(d[k]) 
    return pd.DataFrame(dmaster)


def query_lutetium(query, params):
    conn = get_lutetium_conn()
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return mysql_to_pandas(rows)


#this is soo hacky. I need localforwarding to lutetium
def query_lutetium_ssh(query, file_name):
    cmd = """ssh lutetium "mysql  --defaults-file=~/.my.cnf -e \\" """ +query+ """ \\" --socket  /tmp/mysql.sock"> """+ file_name
    os.system(cmd)
    d = pd.read_csv(file_name,  sep='\t')
    os.system('rm ' + file_name)
    return d

    
def query_hive_ssh(query, file_name):
    cmd = """ssh stat1002.eqiad.wmnet "hive -e \\" """ +query+ """ \\" "> """ + file_name
    os.system(cmd)
    d = pd.read_csv(file_name,  sep='\t')
    os.system('rm ' + file_name)
    return d

def query_stat_ssh(query, file_name):
        cmd = """ssh stat1002.eqiad.wmnet "mysql --defaults-file=/etc/mysql/conf.d/analytics-research-client.cnf -h analytics-store.eqiad.wmnet -u research -e \\" """ +query+ """ \\" --socket  /tmp/mysql.sock  "> """+ file_name
        os.system(cmd)
        d = pd.read_csv(file_name,  sep='\t')
        os.system('rm ' + file_name)
        return d