from db_utils import query_lutetium, get_time_limits
from datetime import timedelta
from plot_utils import plot_df
import pandas as pd

def get_donations(start, stop, campaign):

    """
    Gets all donation data within the time range start:stop
    Groups data by banner, campaign and number of impressions seen
    """
    params = get_time_limits(start, stop)
    params['campaign'] = campaign


    query = """
    SELECT
    DATE_FORMAT(CAST(ts as datetime), '%%Y-%%m-%%d %%H') as timestamp,
    CONCAT_WS(' ', banner, utm_campaign) as name,
    co.total_amount as amount,
    ct.country as country
    FROM civicrm.civicrm_contribution co, drupal.contribution_tracking ct, drupal.contribution_source cs
    WHERE  ct.id = cs.contribution_tracking_id
    AND co.id = ct.contribution_id
    AND ts BETWEEN %(start_ts)s AND %(stop_ts)s
    AND utm_medium = 'sitenotice'
    AND utm_campaign = %(campaign)s 
    """
        
    d = query_lutetium(query, params)
    d.index = d['timestamp'].map(lambda t: pd.to_datetime(str(t)))
    del d['timestamp']
    d['amount'] = d['amount'].fillna(0.0)
    d['amount'] = d['amount'].astype(float)
    
    return d.sort()



def plot_central_tendency(d, regs, start = '2000', stop = '2050', hours = 1, index = None, ylabel = 'dollars per donation',title= '', method = 'mean', amount_limit = 10000.0):
    d = d[start:stop]
    if index is None:
        d.index = pd.Series(d.index).apply(lambda tm: tm - timedelta(hours=(24 * tm.day + tm.hour) % hours))
    else:
        d.index = d[index]

    d_plot = pd.DataFrame()
    for name, reg in regs.items():
        amounts = d.ix[d.name.str.match(reg).apply(bool)]['amount']
        amounts[amounts>amount_limit] = amount_limit 
        if method == 'mean':           
            d_plot[name] = amounts.groupby(amounts.index).sum() / amounts.groupby(amounts.index).count()
        else:
            d_plot[name] = amounts.groupby(amounts.index).median() 
    return plot_df(d_plot, ylabel=ylabel, title=title, interactive = False)