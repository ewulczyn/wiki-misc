
# coding: utf-8

# In[4]:

import MySQLdb
from pprint import pprint
import pandas as pd
from query_remote_from_local_utils import get_lutetium_conn, mysql_to_pandas




def get_cursor(conn, type='dict'):
    if type == 'dict':
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    elif type == 'norm':
        cursor = conn.cursor()
    return cursor


def get_donations_by_medium(cursor, f_year, locatable):

    if locatable:
        is_null = 'not'
    else:
        is_null = ''

    query = "select sum(civi.total_amount) as amount, drupal.utm_medium\
            from civicrm.civicrm_contribution civi left join drupal.contribution_tracking drupal on civi.id = drupal.contribution_id \
            where drupal.country is %s NULL\
            and civi.receive_date < '%s-06-30 23:59:59'\
            and civi.receive_date > '%s-07-01 00:00:00'\
            group by drupal.utm_medium" % (is_null, '%s', '%s')
    
    cursor.execute(query, (f_year, f_year-1, ))
    rows= cursor.fetchall()
    return rows


def get_donations(cursor, f_year, locatable):

    if locatable:
        is_null = 'not'
    else:
        is_null = ''

    query = "select sum(civi.total_amount) as amount\
            from civicrm.civicrm_contribution civi left join drupal.contribution_tracking drupal on civi.id = drupal.contribution_id \
            where drupal.country is %s NULL\
            and civi.receive_date < '%s-06-30 23:59:59'\
            and civi.receive_date > '%s-07-01 00:00:00'" % (is_null, '%s', '%s')
    
    cursor.execute(query, (f_year, f_year-1, ))
    rows= cursor.fetchall()
    return rows 



def mysql_to_pandas(dicts):
    dmaster = {}
    for d in dicts:
        for k in d.keys():
            if k not in dmaster:
                dmaster[k] = []
            
            dmaster[k].append(d[k]) 
    return pd.DataFrame(dmaster)

def get_donations_by_country_of_address(cursor, f_year):


    query = "select sum(total_amount) as total_amount,  count(*) as num_donations, iso_code as country\
    from civicrm_contribution, civicrm_address, civicrm_country\
    where civicrm_contribution.contact_id = civicrm_address.contact_id\
    and civicrm_address.country_id = civicrm_country.id\
    and civicrm_address.is_primary = 1\
    and civicrm_contribution.receive_date < '%s-06-30 23:59:59'\
    and civicrm_contribution.receive_date > '%s-07-01 00:00:00'\
    group by civicrm_country.iso_code"
    print query

    cursor.execute(query, (f_year, f_year-1))

    rows= cursor.fetchall()
    return rows


def group_donations_by_country_of_web_request(cursor,f_year, medium):

    query = "select sum(total_amount) as total_amount,  count(*) as num_donations , country \
        from civicrm.civicrm_contribution civi inner join drupal.contribution_tracking drupal on civi.id = drupal.contribution_id \
        where drupal.country is not NULL\
        and civi.receive_date < '%s-06-30 23:59:59'\
        and civi.receive_date > '%s-07-01 00:00:00'\
        and drupal.utm_medium = %s\
        group by drupal.country;"

    cursor.execute(query, (f_year, f_year-1, medium))
    rows= cursor.fetchall()
    return rows
    
def get_fr_impressions_by_country_of_web_request(cursor, f_year):

    #get banners associated with donations in that f_year
    banners_query = "select distinct( left(substring_index(substring_index(utm_source, '.', 2),'.',1),\
    length(substring_index(substring_index(utm_source, '.', 2),'.',1))))\
    from drupal.contribution_tracking\
    where ts < %s0630235959 and ts >%s0701000000"


    query = "select sum( pgehres.bannerimpressions.count) as num_impressions,\
    UCASE(pgehres.country.iso_code) as country\
    from pgehres.bannerimpressions, pgehres.country\
    where pgehres.country.id = pgehres.bannerimpressions.country_id\
    and pgehres.bannerimpressions.banner in  (select distinct( left(substring_index(substring_index(utm_source, '.', 2),'.',1), length(substring_index(substring_index(utm_source, '.', 2),'.',1))))         from drupal.contribution_tracking         where ts < %s0630235959 and ts >%s0701000000)     and pgehres.bannerimpressions.timestamp < '%s-06-30 23:59:59' and pgehres.bannerimpressions.timestamp > '%s-07-01 00:00:00'     group by pgehres.country.iso_code order by num_impressions desc"

    cursor.execute(query, (f_year, f_year-1, f_year, f_year-1))
    rows= cursor.fetchall()
    return rows


def merge_by_country_code(df):
        common_name = ''
        for index, row in df.iterrows():
            common_name += row['Common Name'] + ", "
        df.ix[df.index[0], 'Common Name'] = common_name[:-2]
        return df.ix[df.index[0]]


def prefix_df_columns(d, prefix, exclude):
    for c in d.columns:
        if c not in exclude:
            d = d.rename(columns = {c: (prefix+c) })
    return d


def get_data_frame_for_media(cursor, f_year):

    d_email = mysql_to_pandas(group_donations_by_country_of_web_request(cursor,f_year, "email"))
    d_email = augemnt_with_total_amount_num_donations_stats(d_email)
    d_email = prefix_df_columns(d_email, 'email_', ['country'])

    print d_email.head()


    d_banner = mysql_to_pandas(group_donations_by_country_of_web_request(cursor,f_year, "sitenotice"))
    d_banner = augemnt_with_total_amount_num_donations_stats(d_banner)
    d_banner = augment_with_impressions_data(d_banner, f_year)
    d_banner = prefix_df_columns(d_banner, 'banner_', ['country'])

    print d_banner.head()

    

    d_sponataneous = mysql_to_pandas(group_donations_by_country_of_web_request(cursor,f_year, "spontaneous"))
    d_sponataneous = augemnt_with_total_amount_num_donations_stats(d_sponataneous)
    d_sponataneous  = prefix_df_columns(d_sponataneous, 'sponataneous_', ['country'])
    print d_sponataneous.head()



    d_sidebar = mysql_to_pandas(group_donations_by_country_of_web_request(cursor,f_year, "sidebar"))
    d_sidebar = prefix_df_columns(d_sidebar, 'sidebar_', ['country'])
    print d_sidebar.head()

    d = d_email.merge(d_sponataneous, how = 'outer', on = 'country').merge(d_sidebar,  how = 'outer', on = 'country').merge(d_banner,  how = 'outer', on = 'country')

    d = augment_with_page_views(d)
    d = augment_with_gdp(d)
    d.to_csv("country_performance_by_channel_"+str(f_year)+".csv")




def get_data_frame_for_all_media(cursor, f_year):
    d = mysql_to_pandas(get_donations_by_country_of_address(cursor,f_year))
    d = augment_with_page_views(d)
    d = augment_with_gdp(d)
    d = augemnt_with_total_amount_num_donations_stats(d)
    d.to_csv("aggregated_country_performance_"+str(f_year)+".csv")



def augement_with_total_amount_num_donations_stats(d):
    d['average_amount'] = d['total_amount'] / d['num_donations']
    d['percent_of_donations'] = 100 * (d['num_donations'].astype(float) / sum(d['num_donations']))
    d['percent_of_amounts'] = 100 * (d['total_amount'] / sum(d['total_amount']))
    return d



def augment_with_impressions_data(d, f_year):
    d_impressions = mysql_to_pandas(get_fr_impressions_by_country_of_web_request(cursor, f_year))
    d = pd.merge(d, d_impressions, how='inner', on='country')
    d['amount_per_1000_impressions'] = 1000*(d['total_amount'] / d['num_impressions'])
    d['donations_per_1000_impressions'] = 1000*(d['num_donations'] / d['num_impressions'])
    return d



def augment_with_page_views(d):
    """mysql --defaults-file=~/.my.cnf -h analytics-store.eqiad.wmnet -u research -e "SELECT country, SUM(pageviews) AS views FROM staging.pageviews where timestamp < '2014-06-30 23:59:59' and timestamp > '2013-07-01 00:00:00' GROUP BY country" > 'pageviews_fy_2014.csv';"""
    pg_fy_2014  = pd.read_csv('pageviews_fy_2014.csv', sep = None)
    d =  pd.merge(d, pg_fy_2014, how='inner', on='country')
    d['percent_of_total_pageviews'] = 100.*(d['pageviews'].astype(float) / sum(d['pageviews']))
    return d



def augment_with_gdp(d):

    country_codes = pd.read_csv('country_codes.csv')
    country_codes  =  country_codes.groupby('ISO 3166-1 2 Letter Code').apply(merge_by_country_code)

    gdp = pd.read_csv('gdp.csv', sep = ",")
    gdp = pd.merge(country_codes, gdp, how = 'inner', left_on='ISO 3166-1 3 Letter Code', right_on = 'Country Code')
    gdp = gdp[['2013', 'Common Name','ISO 3166-1 2 Letter Code' ]]

    d = pd.merge(d, gdp, how = 'inner', left_on = 'country', right_on = 'ISO 3166-1 2 Letter Code')

    del d['country']
    d=d.rename(columns = {'2013':'gdp_ppp'})
    d=d.rename(columns = {'Common Name':'country_name'})
    d=d.rename(columns = {'ISO 3166-1 2 Letter Code':'country_code'})

    return d



if __name__ == '__main__':
    conn = get_lutetium_conn()
    cursor = get_cursor(conn)
    
    get_data_frame_for_media(cursor, 2013)
    get_data_frame_for_media(cursor, 2014)
    get_data_frame_for_all_media(cursor, 2013)
    get_data_frame_for_all_media(cursor, 2014)

    conn.close()




    




