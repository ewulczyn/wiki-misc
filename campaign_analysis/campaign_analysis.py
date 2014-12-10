from db_utils import query_lutetium, get_time_limits
from datetime import timedelta
from plot_utils import plot_df
import pandas as pd



def get_donations(start, stop):

    """
    Gets all donation data within the time range start:stop
    Groups data by banner, campaign and number of impressions seen
    """


    query = """
    SELECT
    DATE_FORMAT(CAST(ts as datetime), '%%Y-%%m-%%d %%H') as timestamp,  CONCAT_WS(' ', banner, utm_campaign) as name,
    COUNT(*) as n,
    SUM(co.total_amount) as amount
    FROM civicrm.civicrm_contribution co, drupal.contribution_tracking ct, drupal.contribution_source cs
    WHERE  ct.id = cs.contribution_tracking_id
    AND co.id = ct.contribution_id
    AND ts BETWEEN %(start_ts)s AND %(stop_ts)s
    AND utm_key is not NULL
    group by DATE_FORMAT(CAST(ts as datetime), '%%Y-%%m-%%d %%H'),  CONCAT_WS(' ', banner, utm_campaign);
    """
    params = get_time_limits(start, stop)
    d = query_lutetium(query, params)
    d.index = d['timestamp'].map(lambda t: pd.to_datetime(str(t)))
    del d['timestamp']
    d['amount'] = d['amount'].fillna(0.0)
    d['amount'] = d['amount'].astype(float)
    
    return d.sort()



def get_impressions(start, stop):

    """
    Gets all donation data within the time range start:stop
    Groups data by banner, campaign and number of impressions seen
    """


    query = """
    SELECT
    DATE_FORMAT(CAST(timestamp as datetime), '%%Y-%%m-%%d %%H') as dt,  CONCAT_WS(' ', banner, campaign) as name, SUM(count) as n  
    FROM pgehres.bannerimpressions 
    WHERE  timestamp BETWEEN %(start)s AND %(stop)s 
    group by DATE_FORMAT(CAST(timestamp as datetime), '%%Y-%%m-%%d %%H'),  CONCAT_WS(' ', banner, campaign);
    """


    params = get_time_limits(start, stop)
    d = query_lutetium(query, params)
    d.index = d['dt'].map(lambda t: pd.to_datetime(str(t)))
    del d['dt']

    d['n'] = d['n'].astype(int)
    
    
    return d.sort()


def plot_by_time(d, regs, start = '2000', stop = '2050', hours = 1, amount = False, cum = False, normalize = False, ylabel = '', interactive = False):
    

    d = d[start:stop]

    d.index = pd.Series(d.index).apply(lambda tm: tm - timedelta(hours=(24 * tm.day + tm.hour) % hours))

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


    #d_plot.plot(figsize=(10, 4))
    return plot_df(d_plot, ylabel, interactive)

def plot_rate_by_time(don, imp, regs,  hours = 1, start = '2000', stop = '2050', ylabel = 'donation rate', interactive = False):
    
    don = don[start:stop]
    imp = imp[start:stop]
    don.index = pd.Series(don.index).apply(lambda tm: tm - timedelta(hours=(24 * tm.day + tm.hour) % hours))
    imp.index = pd.Series(imp.index).apply(lambda tm: tm - timedelta(hours=(24 * tm.day + tm.hour) % hours))


    d_plot = pd.DataFrame()
    for name, reg in regs.items():
        dons = don.ix[don.name.str.match(reg).apply(bool)]['n']
        dons = dons.groupby(dons.index).sum()
        imps = imp.ix[imp.name.str.match(reg).apply(bool)]['n']
        imps = imps.groupby(imps.index).sum()

        d_plot[name] = dons/imps
    #d_plot.plot(figsize=(10, 4))
    return plot_df(d_plot, ylabel, interactive)


