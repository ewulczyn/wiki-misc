from db_utils import query_lutetium, get_time_limits
from datetime import timedelta
from plot_utils import plot_df
import pandas as pd

def get_clicks(start, stop, campaign):

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
    ct.country as country,
    cs.payment_method as payment_method,
    COUNT(*) as n,
    CASE 
        WHEN contribution_id is NULL THEN 0.0
        ELSE 1.0
    END as donation
    FROM drupal.contribution_tracking ct INNER JOIN drupal.contribution_source cs ON ct.id = cs.contribution_tracking_id
    LEFT JOIN civicrm.civicrm_contribution co ON co.id = ct.contribution_id
    WHERE ts BETWEEN %(start_ts)s AND %(stop_ts)s
    AND utm_medium = 'sitenotice'
    AND utm_campaign REGEXP %(campaign)s 
    GROUP BY timestamp, name, donation, country, payment_method
    """

    print query % params
        
    d = query_lutetium(query, params)
    d.index = d['timestamp'].map(lambda t: pd.to_datetime(str(t)))
    del d['timestamp']
    
    
    return d.sort()



def plot_conversion_rate(d, regs, start = '2000', stop = '2050', hours = 1, index = None, ylabel = 'conversion_rate',title= '', methods = ['amazon', 'paypal', 'cc']):
    d = d[start:stop]
    if index is None:
        d.index = pd.Series(d.index).apply(lambda tm: tm - timedelta(hours=(24 * tm.day + tm.hour) % hours))
    else:
        d.index = d[index]

    d_plot = pd.DataFrame()
    for name, reg in regs.items():
        clicks = d.ix[d.name.str.match(reg).apply(bool)]
        for method in methods:
            clicks_by_method = clicks[clicks['payment_method'] == method]
            donations = clicks_by_method[clicks_by_method['donation'] == 1]['n']
            donations = donations.groupby(donations.index).sum()
            clicks_by_method = clicks_by_method.groupby(clicks_by_method.index)['n'].sum()
            

            #print clicks_by_method.head()
            #print donations.head()
            d_plot[name+' '+ method] = donations / clicks_by_method
    return plot_df(d_plot, ylabel=ylabel, title=title, interactive = False)






