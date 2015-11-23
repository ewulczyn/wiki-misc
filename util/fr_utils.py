from db_utils import query_lutetium, get_time_limits
import pandas as pd
from datetime import timedelta


def to_int(x):
    try:
        return int(x)
    except:
        return -1


field_mapping = {
    'campaign': 'utm_campaign',
    'banner': 'banner',
    'name': 'CONCAT_WS(' ', banner, utm_campaign)',
    'timestamp': 'DATE_FORMAT(CAST(ts as datetime), %(time_format)s)',
    'country': 'country',
    'impressions_seen': 'utm_key',
    'payment_method': 'utm_source',
}


def craft_select(fields, field_mapping):
    return "SELECT\n" + ',\n'.join('\t' + [field_mapping[f] + ' AS ' + f for f in fields])

def craft_select(fields, field_mapping):
    return 'GROUP BY\n' + ',\n'.join([ '\t' + field_mapping[f] for f in fields])


def get_clicks(start, stop, campaign_reg = '.*', banner_reg = '.*', aggregation = 'hourly', fields = field_mapping.keys() ):

    """
    Gets all click data within the time range start:stop
    """
    params = get_time_limits(start, stop)
    params['campaign_reg'] = campaign_reg
    params['banner_reg'] = banner_reg
    params['time_format'] = '%Y-%m-%d %H:00:00' if aggregation =='hourly' else '%Y-%m-%d %H:%i:00'



    query = """
    SELECT
        DATE_FORMAT(CAST(ts as datetime), %(time_format)s) as timestamp,
        banner, 
        utm_campaign AS campaign, 
        CONCAT_WS(' ', banner, utm_campaign) as name,
        country,
        CAST(utm_key as int) AS impressions_seen,
        utm_source as payment_method,
        COUNT(*) AS n
    FROM drupal.contribution_tracking ct, drupal.contribution_source cs
        WHERE  ct.id = cs.contribution_tracking_id
        AND ts BETWEEN %(start_ts)s AND %(stop_ts)s
        AND utm_medium = 'sitenotice'
        AND utm_campaign RLIKE %(campaign_reg)s
        AND banner RLIKE %(banner_reg)s
    GROUP BY
        DATE_FORMAT(CAST(ts as datetime), %(time_format)s),
        banner, 
        utm_campaign, 
        country,
        utm_key,
        utm_source
    """

    
    d = query_lutetium(query, params)
    d.index = pd.to_datetime(d['timestamp'])
    del d['timestamp']

    d['impressions_seen'].apply(to_int)
    d['payment_method'] = d['payment_method'].apply(lambda x: x.split('.')[2])

    return d




def get_donations(start, stop, campaign_reg = '.*', banner_reg = '.*', aggregation = 'hourly'):

    """
    Gets all donation data within the time range start:stop
    """
    params = get_time_limits(start, stop)
    params['campaign_reg'] = campaign_reg
    params['banner_reg'] = banner_reg
    params['time_format'] = '%Y-%m-%d %H:00:00' if aggregation =='hourly' else '%Y-%m-%d %H:%i:00'


    query = """
    SELECT
        DATE_FORMAT(CAST(ts as datetime), %(time_format)s) as timestamp,
        banner, 
        utm_campaign AS campaign, 
        CONCAT_WS(' ', banner, utm_campaign) as name,
        country,
        CAST(utm_key as int) AS impressions_seen,
        utm_source as payment_method,
        co.total_amount as amount,
        SUM(co.total_amount) as total_amount,
        COUNT(*) AS n
    FROM civicrm.civicrm_contribution co, drupal.contribution_tracking ct, drupal.contribution_source cs
        WHERE  ct.id = cs.contribution_tracking_id
        AND co.id = ct.contribution_id
        AND ts BETWEEN %(start_ts)s AND %(stop_ts)s
        AND utm_medium = 'sitenotice'
        AND utm_campaign RLIKE %(campaign_reg)s
        AND banner RLIKE %(banner_reg)s
    GROUP BY
        DATE_FORMAT(CAST(ts as datetime), %(time_format)s),
        banner, 
        utm_campaign, 
        country,
        utm_key,
        utm_source,
        co.total_amount
    """

    
    d = query_lutetium(query, params)
    d.index = pd.to_datetime(d['timestamp'])
    del d['timestamp']

    d['impressions_seen'].apply(to_int)
    d['payment_method'] = d['payment_method'].apply(lambda x: x.split('.')[2])

    return d



def get_impressions(start, stop, campaign_reg = '.*',  banner_reg = '.*', aggregation = 'hourly'):

    """
    Gets all impression data within the time range start:stop
    """
    params = get_time_limits(start, stop)
    params['campaign_reg'] = campaign_reg
    params['banner_reg'] = banner_reg
    params['time_format'] = '%Y-%m-%d %H:00:00' if aggregation =='hourly' else '%Y-%m-%d %H:%i:00'



    query = """
    SELECT
        DATE_FORMAT(CAST(timestamp as datetime), %(time_format)s) AS timestamp,
        banner,
        campaign,
        CONCAT_WS(' ', banner, campaign) AS name,
        iso_code AS country, 
        SUM(count) AS n
    FROM pgehres.bannerimpressions imp JOIN pgehres.country c
        WHERE imp.country_id = c.id
        AND timestamp BETWEEN %(start)s AND %(stop)s 
        AND campaign RLIKE %(campaign_reg)s
        AND banner RLIKE %(banner_reg)s
    GROUP BY
        DATE_FORMAT(CAST(timestamp as datetime), %(time_format)s),
        banner,
        campaign,
        iso_code
    """
    
    d = query_lutetium(query, params)
    d.index = pd.to_datetime(d['timestamp'])
    del d['timestamp']
    d['n'] = d['n'].astype(int)
    
    return d


def get_pageviews(start, stop, country, project):

    """
    Get hourly pageview counts fro project from country
    """
    
    query = """
    SELECT
        year,
        month,
        day,
        hour,
        access_method,
        SUM(view_count) as pageviews,
    FROM wmf.projectview_hourly
        WHERE agent_type = 'user'
        AND %(time)s
        AND project = '%(project)s'
        AND country_code = '%(country)s'
    GROUP BY
        year,
        month,
        day,
        hour,
        access_method
    """
    
    params = {'country': country, 'project': project, 'time': get_hive_timespan(start, stop) }
    d = query_hive_ssh(query % params, 'pvquery' + country + project, priority = True, delete = True)
    dt = d["year"].map(str) + '-' + d["month"].map(str) + '-' + d["day"].map(str) + ' ' + d["hour"].map(str) + ':00'
    d.index = pd.to_datetime(dt)

    del d['year']
    del d['month']
    del d['day']
    del d['hour']
    return d