CREATE TEMPORARY FUNCTION geocode_country as 'org.wikimedia.analytics.refinery.hive.GeocodedCountryUDF';
CREATE TEMPORARY FUNCTION parse_ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';


SELECT * FROM

(SELECT
a.country,
a.language_variant,
a.uri_host,
count(*)*64 as n
FROM
(SELECT 
uri_host,
geocode_country(ip) as country,
REGEXP_EXTRACT(uri_path, '/([^/]*)/(.*)', 1) as language_variant
FROM wmf.webrequest TABLESAMPLE(BUCKET 1 OUT OF 64 ON rand())
WHERE
year = 2015
AND month = 1
AND day = 28
AND (uri_host = 'zh.wikipedia.org' OR uri_host = 'zh.m.wikipedia.org')
AND parse_ua(user_agent)['device_family'] != 'Spider'
AND webrequest_source in ('mobile', 'text')
AND is_pageview = 1
) a
WHERE a.country in ('CN', 'HK')
GROUP BY a.country, a.language_variant, a.uri_host) b
ORDER by n DESC LIMIT 100;




SELECT uri_path, count(*)
FROM (
SELECT 
uri_host,
geocode_country(ip) as country,
uri_path,
REGEXP_EXTRACT(uri_path, '/([^/]*)/(.*)', 1) as language_variant
FROM wmf.webrequest TABLESAMPLE(BUCKET 1 OUT OF 64 ON rand())
WHERE
year = 2015
AND month = 1
AND day = 28
AND hour = 1
AND (uri_host = 'zh.wikipedia.org' OR uri_host = 'zh.m.wikipedia.org')
AND parse_ua(user_agent)['device_family'] != 'Spider'
AND webrequest_source in ('mobile', 'text') 
AND REGEXP_EXTRACT(uri_path, '/([^/]*)/(.*)', 1) not in ('w', 'wiki', 'zh-cn', 'zh-hk', 'zh', 'zh-tw', 'zh-sg', 'zh-mo', 'zh-cn', 'zh-hans', 'zh-hant')
AND http_status = 200
) a
group by uri_path;