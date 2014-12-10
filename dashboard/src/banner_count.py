from data_retrieval import OldBannerDataRetriever
from query_remote_from_local_utils import *
import matplotlib.pyplot as plt


# Over a given time interval

# Look at impressions seen by reader

# Look at impressions donors

# donation rate as a function of imprssions seen

# survival function

def get_impressions_by_banner_count(start, stop):

    """
    Gets all impression data within the time range start:stop
    Groups data by banner, campaign and number of impressions seen
    """

    data_dict = {}
    params = OldBannerDataRetriever(None,  start, stop).params
    query = """
    SELECT impressions_seen, CONCAT_WS(' ', banner, campaign, day) as name, n
    FROM ellery.banner_count
    WHERE day BETWEEN '%(start)s' AND '%(stop)s';
    """



    query += ";"
    query = query % {'start':start, 'stop':stop}
    d = query_hive_ssh(query, 'impressions_by_count.tsv') 

    d.index = d.impressions_seen 
    d.drop('impressions_seen', axis=1, inplace=True)


    return d


def get_donations_by_banner_count(start, stop):

    """
    Gets all donation data within the time range start:stop
    Groups data by banner, campaign and number of impressions seen
    """


    query = """
    SELECT
    cast(ct.utm_key as int) as impressions_seen,
    CONCAT_WS(' ', banner, utm_campaign, substr(DATE_FORMAT(CAST(ts as datetime), '%%Y-%%m-%%d'), 1, 10)) as name,
    COUNT(*) as n,
    SUM(co.total_amount) as amount
    FROM civicrm.civicrm_contribution co, drupal.contribution_tracking ct, drupal.contribution_source cs
    WHERE  ct.id = cs.contribution_tracking_id
    AND co.id = ct.contribution_id
    AND ts BETWEEN %(start_ts)s AND %(stop_ts)s
    AND utm_key is not NULL
    group by ct.utm_key, CONCAT_WS(' ', banner, utm_campaign, substr(DATE_FORMAT(CAST(ts as datetime), '%%Y-%%m-%%d'), 1, 10));
    """
    params = OldBannerDataRetriever(None,  start, stop).params
    d = query_lutetium(query, params)
    d['impressions_seen'] = d['impressions_seen'].fillna(-1)
    d['impressions_seen'] = d['impressions_seen'].astype(int)
    
    d.index = d.impressions_seen 
    d.drop('impressions_seen', axis=1, inplace=True)
    
    return d.sort()






def plot_by_impressions_seen(d, regs, normalize = True, max_impressions = 5, amount = False):
    

    
    d_plot = pd.DataFrame()
    for name, reg in sorted(regs.items()):
        counts = d[d.name.str.contains(reg)]['n']
        counts = counts.groupby(counts.index).sum()
        if normalize:
            counts = counts / counts.sum()

        counts1 = counts.loc[:max_impressions]
        counts1.loc[max_impressions+1] = counts[max_impressions:].sum()
        d_plot[name] = counts1

    return plot_df(d_plot)


def plot_donation_rate(don, imp, regs, cum = False,max_impressions=5 ):
    

    

    d_plot = pd.DataFrame()
    for name, reg in sorted(regs.items()):


        donations = don.ix[don.name.str.contains(reg)]['n']
        donations = donations.groupby(donations.index).sum()
        donations1 = donations.loc[:max_impressions]
        donations1.loc[max_impressions+1] = donations[max_impressions:].sum()

        impressions = imp.ix[imp.name.str.contains(reg)]['n']
        impressions = impressions.groupby(impressions.index).sum()
        impressions1 = impressions.loc[:max_impressions]
        impressions1.loc[max_impressions+1] = impressions[max_impressions:].sum()

        if cum:

            impressions1 = impressions1.cumsum()
            donations1 = donations1.cumsum()




        d_plot[name] = donations1 / impressions1

    return plot_df(d_plot)




def plot_df(d):
    fig = plt.figure(figsize=(10, 4), dpi=80)
    for c in d.columns:
        plt.plot(d.index, d[c], label = c)
    plt.legend()
    return fig




