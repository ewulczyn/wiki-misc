select sum(n) from oozie_mc_v0_4 where month=1 and curr = 'Amateur_astronomy';

select prev, curr, n, day from oozie_mc_v0_4 where month=1 and curr = 'Amateur_astronomy' order by n desc limit 300;



select sum(n) from oozie_mc_v0_4 where month=1 and prev = 'Amateur_astronomy';

select prev, curr, n, day from oozie_mc_v0_4 where month=1 and prev = 'Amateur_astronomy' order by n desc limit 300;

CREATE TEMPORARY FUNCTION  resolve_ip as 'org.wikimedia.analytics.refinery.hive.ClientIpUDF';

select dt, resolve_ip(ip, x_forwarded_for), user_agent, referer, uri_path from wmf.webrequest where year=2015 and month=1 and day=14 and referer like '%Amateur_astronomy' and uri_host = 'en.wikipedia.org' and is_pageview order by dt limit 10000;


SELECT ip, user_agent, n FROM
(select resolve_ip(ip, x_forwarded_for) as ip, user_agent, count(*) as n
 from wmf.webrequest
 where webrequest_source = 'text'
 AND parse_ua(user_agent)['device_family'] != 'Spider'
AND is_crawler(user_agent) = 0
AND user_agent NOT RLIKE 'bot|Bot|spider|Spider|crawler|Crawler|http|Scraper|scraper'
AND user_agent NOT RLIKE 'HTTrack|AppleDictionaryService|Twisted PageGetter|Akamai SureRoute|WikiWand|WordPress|MediaWiki'

 AND year=2015 and month=1 and day=14 and hour = 5
 and is_pageview
  group by resolve_ip(ip, x_forwarded_for), user_agent having count(*) > 10 ) a
order by n desc limit 5000;