import os
import pandas as pd
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt
from numpy.random import  beta

pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)





def query_lutetium(query):
    cmd = """ssh lutetium "mysql  --defaults-file=~/.my.cnf -e \\" """ +query+ """ \\" --socket  /tmp/mysql.sock --database civicrm "> query_lutetium.csv"""
    os.system(cmd)
    d = pd.read_csv('query_lutetium.csv',  sep='\t')
    os.system('rm query_lutetium.csv',)
    return d


def query_hive(query):
    cmd = """ssh stat1002.eqiad.wmnet "hive -S -e \\" """ +query+ """ \\" "> query_lutetium.csv"""
    os.system(cmd)
    d = pd.read_csv('query_lutetium.csv',  sep='\t')
    os.system('rm query_lutetium.csv',)
    return d



def get_donation_pages(banner):
	query = "select\
    referrer, count(*) as num_donations \
    from drupal.contribution_tracking \
	where left(substring_index(substring_index(utm_source, '.', 2),'.',1), length(substring_index(substring_index(utm_source, '.', 2),'.',1))) = '"+banner+"'\
	and contribution_id is not NULL \
	group by referrer order by num_donations desc;"
	d = query_lutetium(query)
	return d


def get_click_pages(banner):
	query = "select\
    referrer as referer, count(*) as num_clicks \
    from drupal.contribution_tracking \
	where left(substring_index(substring_index(utm_source, '.', 2),'.',1), length(substring_index(substring_index(utm_source, '.', 2),'.',1))) = '"+banner+"'\
	group by referrer order by num_clicks desc;"
	d = query_lutetium(query)
	return d

def get_impression_pages(banner):
	query = "select referer, count(*) as num_impressions \
	from wmf_raw.webrequest where  year = 2014 and month = 10 and day = 3 and hour = 14 \
	and parse_url(concat('http://bla.org/woo/', uri_query), 'QUERY', 'banner') = '"+banner+"'\
	group by referer order by num desc limit 2000;"
	d = query_hive(query)
	return d


def get_p_dist(num_donations, num_impressions):
	return beta(num_donations+1, num_impressions-num_donations+1, 5000)

def ci(dist, conf):
	return (np.percentile(dist, (100 - conf)/2 ), np.percentile(dist, conf + (100 - conf)/2 ))


def lb_click(x):
	dist = get_p_dist(x['num_clicks'], x['num_impressions'])
	return ci(dist, 90)[0]

def top_pages(banner):
	d_impressions = pd.read_csv('impression_pages.tsv',  sep='\t')
	#d_impressions = get_impression_pages(banner)
	d_clicks = get_click_pages(banner)
	d = pd.merge(d_impressions, d_clicks)
	d = d.ix[d['num_impressions'] > 100]
	d['prob_click'] = d['num_clicks']/d['num_impressions']
	d['lb'] = d.apply(lb_click, axis = 1)
	d = d.sort(['lb', ], ascending=[0,])
	print d.head(100)


if __name__ == '__main__':
	#get_impression_pages('B14_1003_enUS_dsk_rpt_1')
	#get_donation_pages('B14_1003_enUS_dsk_rpt_1')
	top_pages('B14_1003_enUS_dsk_rpt_1')