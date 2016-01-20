from db_utils import query_hive_ssh, execute_hive_expression, get_hive_timespan
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import shutil
import os
import numpy as np

def get_country_project_condition(d):
        pairs = []
        for c,pl in d.items():
            for p in pl:
                pairs.append("(project = '%s.wikipedia' AND country = '%s')" % (p,c))
        return "(" + " OR ".join(pairs) + ")"


def create_hive_ts(d, start, stop):
    
    query = """
        DROP TABLE IF EXISTS censorship.daily_ts2;
        CREATE TABLE censorship.daily_ts2
        AS SELECT 
            CONCAT(ts.year,'-',LPAD(ts.month,2,'0'),'-',LPAD(ts.day,2,'0')) as day,
            ts.country, 
            ts.project, 
            ts.page_title,
            ts.n,
            ts.n / agg.n_agg as proportion,
            wd.en_page_title
        FROM 
            (SELECT
                year, 
                month, 
                day, 
                country, 
                project, 
                page_title,
                SUM(view_count) as n
            FROM wmf.pageview_hourly
                WHERE agent_type = 'user'
                AND page_title not RLIKE ':'
                AND %(cp_conditions)s
                AND %(time_conditions)s
            GROUP BY
                year,
                month,
                day,
                country,
                project,
                page_title
            ) ts
        LEFT JOIN
            (SELECT
                year, 
                month, 
                day, 
                project, 
                page_title,
                SUM(view_count) as n_agg
            FROM wmf.pageview_hourly
                WHERE agent_type = 'user'
                AND page_title not RLIKE ':'
                AND %(time_conditions)s
            GROUP BY
                year,
                month,
                day,
                project,
                page_title
            ) agg
            ON (    ts.year = agg.year
                AND ts.month = agg.month
                AND ts.day = agg.day
                AND ts.project = agg.project
                AND ts.page_title = agg.page_title)
        LEFT JOIN censorship.wikidata wd
            ON (ts.page_title = wd.page_title AND ts.project = wd.project);
    """
    params = {'cp_conditions' : get_country_project_condition(cp_dict),
              'time_conditions': get_hive_timespan(start, stop),
              }
    query %= params
    query_hive_ssh(query, 'ts', priority = True)


def create_hive_view_proportion_deltas(transition_date, n_pre, n_post):
    query = """
    DROP TABLE IF EXISTS censorship.deltas;
    CREATE TABLE censorship.deltas
     AS SELECT
        (post.proportion - pre.proportion) / pre.proportion AS delta,
        post.country, 
        post.project, 
        post.page_title
    FROM
    (SELECT 
        SUM(proportion) / %(n_pre)d AS proportion,
        country, 
        project, 
        page_title
    FROM censorship.daily_ts2
        WHERE day < '%(date)s'
    GROUP BY 
        country, 
        project, 
        page_title) pre  
    JOIN 
    (SELECT 
        SUM(proportion)/%(n_post)d AS proportion,
        country, 
        project, 
        page_title
    FROM censorship.daily_ts2
        WHERE day >= '%(date)s'
    GROUP BY 
        country, 
        project, 
        page_title
        ) post
    ON (
        pre.country = post.country
        AND pre.project = post.project
        AND pre.page_title = post.page_title
    )
    """
    
    query %= {'date': transition_date, 'n_pre': n_pre, 'n_post': n_post}
    query_hive_ssh(query, 'ts', priority = True)



def query_country_deltas(country, n): # This is broken!
    query = """
    SELECT 
        country,
        project,
        page_title,
        delta
    FROM censorship.deltas
    WHERE country = '%s'
    ORDER BY delta DESC
    LIMIT %d
    """
    return query_hive_ssh(query % (country, n), 'deltas.tsv')

def get_id_conditions(ids, en_title = True):
    condidions = []
    for d in ids:
        if en_title:
            condidions.append("(project = '%(project)s' AND country = '%(country)s' AND en_page_title = '%(en_page_title)s')" % d)
        else:
            condidions.append("(project = '%(project)s' AND country = '%(country)s' AND page_title = '%(page_title)s')" % d)
    return "(" + " OR ".join(condidions) + ")"


     
def get_local_ts(ids, en_titles = True):
    
    params = {
        'id_condition': get_id_conditions(ids, en_title = en_titles),
    }

    query = """
    SELECT *
    FROM censorship.daily_ts2
    WHERE %(id_condition)s
    """

    df =  query_hive_ssh(query % params, 'ts', priority = True)
    df.columns = [c.split('.')[1] for c in df]
    df.index  = pd.to_datetime(df.day)
    return df



def get_single_ts(df_ts, start, stop, id_dict, field):
    indices = (df_ts['country'] == id_dict['country']) & (df_ts['en_page_title'] == id_dict['en_page_title']) & (df_ts['project'] == id_dict['project'])
    data = df_ts[indices]
    ts = pd.Series(data[field], index = pd.date_range(start=start, end=stop, freq='d') )
    ts.fillna(0, inplace = True)
    return ts


def plot_series(df, start, stop, id_dict, smooth = 1):
    ts = get_single_ts(df, start, stop, id_dict, 'n')
    ts = pd.rolling_mean(ts, smooth)

    ts_prop = get_single_ts(df, start, stop, id_dict, 'proportion')
    ts_prop = pd.rolling_mean(ts_prop, smooth)
    
    f, axarr = plt.subplots(2, sharex=True)
    
    english_end = datetime.strptime('2015-06-12 09:40', "%Y-%m-%d %H:%M") # End transition of English Wikipedia, including Mobile

    # plot transition point
    axarr[0].axvline(english_end, color='blue', label = 'HTTPS transition', linewidth=0.5)
    axarr[1].axvline(english_end, color='blue', label = 'HTTPS transition', linewidth=0.5)
    
    axarr[0].plot(ts.index, ts.values)
    axarr[1].plot(ts_prop.index, ts_prop.values)

    return ts, ts_prop, f



def get_id(id_dict):
    return id_dict['en_page_title'] + '_' + id_dict['project'] + '_' + id_dict['country']


def save_series_plot(df, start, stop, id_dict, fig_dir, smooth = 1):
    ts, ts_prop, f = plot_series(df, start, stop, id_dict, smooth = smooth)
    fig_name = get_id(id_dict) + '.pdf'
    fig_name = fig_name.replace('/', '-')
    plt.savefig(os.path.join(fig_dir, fig_name))
    plt.close(f)    

    
def save_all_series_plots(df, start, stop, id_dicts, fig_dir, smooth = 1):
    if os.path.exists(fig_dir):
        shutil.rmtree(fig_dir)
    os.makedirs(fig_dir)
    for d in id_dicts:
        plot_series(df, start, stop, d, fig_dir, smooth = smooth)


def query_deltas(cmp):
    """
    Pull a dataframe with popular articles where pageviews at least doubles
    """
    query = """
    SELECT *,
    (post_n_tpc / 3 - pre_n_tpc) / pre_n_tpc as clean_tpc_view_delta
    FROM censorship.20150515_20150528__20150617_20150730
    WHERE post_n_tpc > 300
    AND pre_n_wd > 1000
    AND (post_n_tpc / 3 - pre_n_tpc) / pre_n_tpc > 2.0
    """
    df = query_hive_ssh(query , 'get_PVSpanComparison_df', priority = True)
    df.columns = [c.split('.')[1] if len(c.split('.')) == 2 else c for c in df.columns]
    df.sort('normalized_tpc_view_proportion_delta', inplace  = True, ascending = 0)
    return df

def write_ts_to_file(id_dict, ts, ts_prop, label, f):
    s = '\t'.join(ts.astype(str))
    s += '\t' + '\t'.join(ts_prop.astype(str))
    s += '\t' + '\t'.join([str(v) for v in id_dict.values() if not isinstance(v, str)])
    s += '\t' + label + '\n'
    f.write(s)
