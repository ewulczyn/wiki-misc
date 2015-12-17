import os
import pandas as pd
from db_utils import get_time_limits
import dateutil.parser
from dateutil import relativedelta
from db_utils import query_hive_ssh, execute_hive_expression, get_hive_timespan
import copy


"""
CREATE EXTERNAL TABLE ellery.wikidata(id STRING, lang STRING, page_title STRING ) ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE
LOCATION '/user/ellery/translation-recs-app/data/wikidata/WILLs';
"""

def create_wikidata_table(db):
    params = {'db': db}
    query = """
    DROP TABLE IF EXISTS %(db)s.wikidata;
    CREATE TABLE %(db)s.wikidata AS
    SELECT wikidata.id,
    CONCAT(wikidata.lang, '.wikipedia') as project,
    regexp_replace(wikidata.page_title, ' ', '_') as page_title,
    regexp_replace(en_wikidata.en_page_title, ' ', '_') as en_page_title
    FROM ellery.wikidata wikidata
    LEFT JOIN
    (SELECT id, regexp_replace(page_title, ' ', '_') as en_page_title
    FROM ellery.wikidata
    WHERE lang == 'en') en_wikidata
    ON (wikidata.id == en_wikidata.id);
    """
    execute_hive_expression(query % params)



class PVSpan:
    def __init__(self, start, stop, db, dry = False):
        
        basename =  start.replace('-', '') + '_' + stop.replace('-', '')
        
        self.params = {
            'basename': basename,
            'start': start,
            'stop': stop,
            'tpc_table' : basename + '_tpc',
            'wdc_table': basename + '_wdc',
            'tp_table' : basename + '_tp',
            'wd_table' : basename + '_wd',
            'c_table' : basename + '_c',
            'db': db,
            'time_conditon': get_hive_timespan(start, stop)
        }
        
        if not dry:
            self.create_tpc_table()
            self.create_wdc_table()
            self.create_tp_table()
            self.create_wd_table()
            self.create_c_table()
            self.join_and_clean()
        
    def create_tpc_table(self):

        query = """
        DROP TABLE IF EXISTS %(db)s.%(tpc_table)s;
        CREATE TABLE %(db)s.%(tpc_table)s AS
        SELECT
        tbl.*,
        wd.id, 
        wd.en_page_title
        FROM
        (
            SELECT
            SUM(view_count) as n_tpc,
            regexp_replace(page_title, ' ', '_') as t,
            country as c,
            project as p
            FROM wmf.pageview_hourly
            WHERE agent_type = 'user'
            AND project RLIKE 'wikipedia'
            AND page_title not RLIKE ':'
            AND %(time_conditon)s
            GROUP BY regexp_replace(page_title, ' ', '_'), country, project
        ) tbl
        LEFT JOIN
        %(db)s.wikidata wd
        ON (tbl.t = wd.page_title AND tbl.p = wd.project);
        """ 
        
        execute_hive_expression(query % self.params) 
        
        
    def create_wdc_table(self):
   
        query = """
        DROP TABLE IF EXISTS %(db)s.%(wdc_table)s;
        CREATE TABLE %(db)s.%(wdc_table)s AS
        SELECT
        SUM(n_tpc) as n_wdc, c, id
        FROM %(db)s.%(tpc_table)s
        GROUP BY c, id
        """
        execute_hive_expression(query % self.params)
        
        
        
    def create_tp_table(self):
        
        query = """
        DROP TABLE IF EXISTS %(db)s.%(tp_table)s;
        CREATE TABLE %(db)s.%(tp_table)s AS
        SELECT
        SUM(n_tpc) as n_tp, t, p
        FROM %(db)s.%(tpc_table)s
        GROUP BY t, p
        """
        execute_hive_expression(query % self.params) 
        
    
    def create_wd_table(self):
        
        query = """
        DROP TABLE IF EXISTS %(db)s.%(wd_table)s;
        CREATE TABLE %(db)s.%(wd_table)s AS
        SELECT
        SUM(n_tpc) as n_wd, id
        FROM %(db)s.%(tpc_table)s
        GROUP BY id
        """
        execute_hive_expression(query % self.params) 
        
        
    
        
    def create_c_table(self):
   
        query = """
        DROP TABLE IF EXISTS %(db)s.%(c_table)s;
        CREATE TABLE %(db)s.%(c_table)s AS
        SELECT
        SUM(n_tpc) as n_c, c
        FROM %(db)s.%(tpc_table)s
        GROUP BY c
        """
        execute_hive_expression(query % self.params)
    
    def join_and_clean(self):
        query = """
        DROP TABLE IF EXISTS %(db)s.%(basename)s;
        CREATE TABLE %(db)s.%(basename)s AS
        SELECT 
        tpc.*,
        wdc.n_wdc,
        tp.n_tp as n_tp,
        wd.n_wd as n_wd,
        c_.n_c as n_c
        FROM 
        %(db)s.%(tpc_table)s tpc,
        %(db)s.%(wdc_table)s wdc,
        %(db)s.%(tp_table)s tp,
        %(db)s.%(wd_table)s wd,
        %(db)s.%(c_table)s c_
        WHERE tpc.c = c_.c
        AND tpc.t = tp.t
        AND tpc.p = tp.p
        AND tpc.c = wdc.c
        AND tpc.id = wdc.id
        AND tpc.id = wd.id;
        
        DROP TABLE IF EXISTS %(db)s.%(tpc_table)s;
        DROP TABLE IF EXISTS %(db)s.%(wdc_table)s;
        DROP TABLE IF EXISTS %(db)s.%(tp_table)s;
        DROP TABLE IF EXISTS %(db)s.%(wd_table)s;
        DROP TABLE IF EXISTS %(db)s.%(c_table)s;
        """
        execute_hive_expression(query % self.params)


class PVSpanComparison:
    
    def __init__(self, pre_span, post_span, db, dry = False):
        
        pre_pvspan = PVSpan(pre_span[0], pre_span[1], db, dry)
        post_pvspan = PVSpan(post_span[0], post_span[1], db, dry)
        self.params = {
            'db': db,
            'pre' : pre_pvspan.params['basename'],
            'post' : post_pvspan.params['basename'],
            'span_table': pre_pvspan.params['basename'] + '__' + post_pvspan.params['basename']
        }
        
        if not dry:
            self.merge_spans()
            self.get_pt_sufficient_statistics()
            self.get_wd_sufficient_statistics()
        
       
    def merge_spans(self):
    
        query = """
        DROP TABLE IF EXISTS %(db)s.%(span_table)s;
        CREATE TABLE %(db)s.%(span_table)s AS
        SELECT
        (post.n_wdc - pre.n_wdc) / pre.n_wdc as wdc_view_delta,
        (((10000 * post.n_wdc) / post.n_c) - ((10000 * pre.n_wdc) / pre.n_c)) / ((10000 * pre.n_wdc) / pre.n_c) as wdc_view_proportion_delta,
        (post.n_tpc - pre.n_tpc) / pre.n_tpc as tpc_view_delta,
        (((10000 * post.n_tpc) / post.n_c) - ((10000 * pre.n_tpc) / pre.n_c)) / ((10000 * pre.n_tpc) / pre.n_c) as tpc_view_proportion_delta,
        pre.n_tpc as pre_n_tpc,
        post.n_tpc as post_n_tpc,
        pre.n_wdc as pre_n_wdc,
        post.n_wdc as post_n_wdc,
        pre.n_tp as pre_n_tp,
        post.n_tp as post_n_tp,
        pre.n_wd as pre_n_wd,
        post.n_wd as post_n_wd,
        pre.n_c as pre_n_c,
        post.n_c as post_n_c,
        post.p,
        post.t,
        post.c,
        post.id,
        post.en_page_title
        FROM
        %(db)s.%(post)s post
        LEFT JOIN
        %(db)s.%(pre)s pre
        ON (pre.t = post.t AND pre.c = post.c AND pre.p = post.p)
        """
        execute_hive_expression(query % self.params)
        
    def get_pt_sufficient_statistics(self):
        metric = 'tpc_view_proportion_delta'
        params = self.params
        params['metric'] = metric
        params['ss_table'] = '_'.join(['ss', metric])

        query = """
        DROP TABLE IF EXISTS %(db)s.%(span_table)s_temp;
        ALTER TABLE %(db)s.%(span_table)s RENAME TO %(db)s.%(span_table)s_temp;
        DROP TABLE IF EXISTS %(db)s.%(span_table)s;
        CREATE TABLE %(db)s.%(span_table)s AS SELECT
        tbl.*,
        (tbl.%(metric)s - ss.mu) / ss.sigma as normalized_%(metric)s
        FROM
        %(db)s.%(span_table)s_temp tbl
        LEFT JOIN
        (SELECT
        t, p,
        SUM(%(metric)s) / COUNT(*) as mu,
        SQRT( SUM(POW(%(metric)s, 2)) / COUNT(*) - POW(SUM(%(metric)s) / COUNT(*), 2) ) as sigma
        FROM %(db)s.%(span_table)s_temp
        GROUP BY p, t) ss
        ON (tbl.t = ss.t and tbl.p = ss.p);
        DROP TABLE %(db)s.%(span_table)s_temp;
        """   

        execute_hive_expression(query % params)


    def get_wd_sufficient_statistics(self):
        metric = 'wdc_view_proportion_delta'
        params = self.params
        params['metric'] = metric
        params['ss_table'] = '_'.join(['ss', metric])

        query = """
        DROP TABLE IF EXISTS %(db)s.%(span_table)s_temp;
        ALTER TABLE %(db)s.%(span_table)s RENAME TO %(db)s.%(span_table)s_temp;
        DROP TABLE IF EXISTS %(db)s.%(span_table)s;
        CREATE TABLE %(db)s.%(span_table)s AS SELECT
        tbl.*,
        (tbl.%(metric)s - ss.mu) / ss.sigma as normalized_%(metric)s
        FROM
        %(db)s.%(span_table)s_temp tbl
        LEFT JOIN
        (SELECT
        id,
        SUM(%(metric)s) / COUNT(*) as mu,
        SQRT( SUM(POW(%(metric)s, 2)) / COUNT(*) - POW(SUM(%(metric)s) / COUNT(*), 2) ) as sigma
        FROM %(db)s.%(span_table)s_temp
        GROUP BY id) ss
        ON (tbl.id = ss.id);
        DROP TABLE %(db)s.%(span_table)s_temp;
        """   

        execute_hive_expression(query % params)


def check_normalization(pvsc, article):
    params = copy.copy(pvsc.params)
    params['article'] = article
    query = """
    SELECT *
    FROM %(db)s.%(span_table)s
    WHERE en_page_title = '%(article)s'
    """
    df = query_hive_ssh(query % params, 'get_PVSpanComparison_df')
    df.columns = [c.split('.')[1] for c in df.columns]
    df.sort('normalized_wdc_view_proportion_delta', inplace  = True, ascending = 0)
    return df









        