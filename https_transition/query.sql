hive -e \
" \
SELECT  \
year, \
month, \
day, \
hour, \
uri_host, \
geocoded_data['country'] as country, \
access_method, \
agent_type, \
x_analytics_map['https'] as https, \
http_status, \
count(*) as n \
FROM wmf.webrequest TABLESAMPLE(BUCKET 1 OUT OF 64 ON rand()) \
WHERE \
year = 2015 \
AND month = 6 \
AND webrequest_source in ('mobile', 'text') \
AND is_pageview = 1 \
AND uri_host RLIKE '(ca|en|zh|it|ug|he)\\.(m\\.)?wikipedia' \
GROUP BY uri_host, geocoded_data['country'], x_analytics_map['https'], http_status, \
access_method, agent_type, year, month, day, hour;" \
> http_transition4.tsv 






set mapreduce.job.queuename=priority;
set hive.exec.reducers.max=30;
set hive.exec.mappers.max=30;

select x_analytics_map['https'] from webrequest where year=2015 and month = 6 and day = 12 and hour = 15 and length(x_analytics) > 1 limit 100;

hive -e "SELECT * from joal.pageviews_hourly where year = 2015 and month = 6;" > http_transition3.tsv 

