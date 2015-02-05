from db_utils import query_lutetium, get_time_limits
from datetime import timedelta
from plot_utils import plot_df
import pandas as pd


def get_clicks(start, stop, campaign = '.*'):

    """
    Gets all donation data within the time range start:stop
    Groups data by banner, campaign and number of impressions seen
    """
    params = get_time_limits(start, stop)
    params['campaign'] = campaign


    query = """
    SELECT
    DATE_FORMAT(CAST(ts as datetime), '%%Y-%%m-%%d %%H') as timestamp,  CONCAT_WS(' ', banner, utm_campaign) as name,
    COUNT(*) as n,
    ct.country as country
    FROM drupal.contribution_tracking ct, drupal.contribution_source cs
    WHERE  ct.id = cs.contribution_tracking_id
    AND ts BETWEEN %(start_ts)s AND %(stop_ts)s
    AND utm_medium = 'sitenotice'
    AND utm_campaign REGEXP %(campaign)s
    GROUP BY DATE_FORMAT(CAST(ts as datetime), '%%Y-%%m-%%d %%H'),  CONCAT_WS(' ', banner, utm_campaign)
    """
    
    print query % params
    d = query_lutetium(query, params)
    print d.head()
    d.index = d['timestamp'].map(lambda t: pd.to_datetime(str(t)))
    del d['timestamp']
    
    return d.sort()




def get_donations(start, stop, campaign = '.*'):

    """
    Gets all donation data within the time range start:stop
    Groups data by banner, campaign and number of impressions seen
    """
    params = get_time_limits(start, stop)
    params['campaign'] = campaign


    query = """
    SELECT
    DATE_FORMAT(CAST(ts as datetime), '%%Y-%%m-%%d %%H') as timestamp,  CONCAT_WS(' ', banner, utm_campaign) as name,
    COUNT(*) as n,
    SUM(co.total_amount) as amount,
    ct.country as country
    FROM civicrm.civicrm_contribution co, drupal.contribution_tracking ct, drupal.contribution_source cs
    WHERE  ct.id = cs.contribution_tracking_id
    AND co.id = ct.contribution_id
    AND ts BETWEEN %(start_ts)s AND %(stop_ts)s
    AND utm_medium = 'sitenotice'
    AND utm_campaign REGEXP %(campaign)s
    GROUP BY DATE_FORMAT(CAST(ts as datetime), '%%Y-%%m-%%d %%H'),  CONCAT_WS(' ', banner, utm_campaign)
    """
    
    print query % params
    d = query_lutetium(query, params)
    print d.head()
    d.index = d['timestamp'].map(lambda t: pd.to_datetime(str(t)))
    del d['timestamp']
    d['amount'] = d['amount'].fillna(0.0)
    d['amount'] = d['amount'].astype(float)
    
    return d.sort()



def get_impressions(start, stop, country_id = None):

    """
    Gets all donation data within the time range start:stop
    Groups data by banner, campaign and number of impressions seen
    """
    params = get_time_limits(start, stop)
    params['country_id'] = country_id


    query = """
    SELECT
    DATE_FORMAT(CAST(timestamp as datetime), '%%Y-%%m-%%d %%H') as dt,  CONCAT_WS(' ', banner, campaign) as name, SUM(count) as n  
    FROM pgehres.bannerimpressions 
    WHERE  timestamp BETWEEN %(start)s AND %(stop)s
    """
    if country_id:
        query += " AND country_id = %(country_id)s"

    query += " GROUP BY DATE_FORMAT(CAST(timestamp as datetime), '%%Y-%%m-%%d %%H'),  CONCAT_WS(' ', banner, campaign)"
    
    d = query_lutetium(query, params)
    d.index = d['dt'].map(lambda t: pd.to_datetime(str(t)))
    del d['dt']
    d['n'] = d['n'].astype(int)
    
    
    return d.sort()


def plot_by_time(d, regs, start = '2000', stop = '2050', hours = 1, amount = False, cum = False, normalize = False, ylabel = '', interactive = False, index = None):
    
    d = d[start:stop]

    if index is None:
        d.index = pd.Series(d.index).apply(lambda tm: tm - timedelta(hours=(24 * tm.day + tm.hour) % hours))
    else:
        d.index = d[index]

    d_plot = pd.DataFrame()
    for name, reg in regs.items():
        if amount:
            counts = d.ix[d.name.str.match(reg).apply(bool)]['amount']
        else:
            counts = d.ix[d.name.str.match(reg).apply(bool)]['n']

        if normalize:
            counts = counts/counts.sum()

        if cum:
            d_plot[name] = counts.groupby(counts.index).sum().cumsum()
        else:
            d_plot[name] = counts.groupby(counts.index).sum()

    d_plot = d_plot.fillna(0)
    #d_plot.plot(figsize=(10, 4))
    return plot_df(d_plot, ylabel, interactive = interactive)



def plot_rate_by_time(don, imp, regs,  hours = 1, start = '2000', stop = '2050', ylabel = 'donation rate', interactive = False, index = None):
    

    don = don[start:stop]
    imp = imp[start:stop]

    if index is None:
        don.index = pd.Series(don.index).apply(lambda tm: tm - timedelta(hours=(24 * tm.day + tm.hour) % hours))
        imp.index = pd.Series(imp.index).apply(lambda tm: tm - timedelta(hours=(24 * tm.day + tm.hour) % hours))
    else:
        don.index = don[index]
        imp.index = imp[index]

    d_plot = pd.DataFrame()
    for name, reg in regs.items():
        dons = don.ix[don.name.str.match(reg).apply(bool)]['n']
        dons = dons.groupby(dons.index).sum()
        imps = imp.ix[imp.name.str.match(reg).apply(bool)]['n']
        imps = imps.groupby(imps.index).sum()

        d_plot[name] = dons/imps
    #d_plot.plot(figsize=(10, 4))
    return plot_df(d_plot, ylabel, interactive = interactive)


