USE ellery;

ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION parse_ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
CREATE TEMPORARY FUNCTION geocode as 'org.wikimedia.analytics.refinery.hive.GeocodedCountryUDF';
CREATE TEMPORARY FUNCTION is_crawler as 'org.wikimedia.analytics.refinery.hive.IsCrawlerUDF';
CREATE TEMPORARY FUNCTION get_access_method as 'org.wikimedia.analytics.refinery.hive.GetAccessMethodUDF';
CREATE TEMPORARY FUNCTION  resolve_ip as 'org.wikimedia.analytics.refinery.hive.ClientIpUDF';




CREATE TEMPORARY MACRO get_project(uri_host STRING)
    reverse(split(reverse(uri_host), '\\.')[1]);

CREATE TEMPORARY MACRO get_variant(uri_host STRING)
    REGEXP_EXTRACT(uri_host, '(www\\.)?(((?:(?!m\\.|zero\\.|wap\\.|mobile\\.)[^.])*)\\.)?((m|zero|wap|mobile)\\.)?(wikipedia|wiktionary|wikibooks|wikinews|wikiquote|wikisource|wikiversity|wikivoyage|wikimedia|wikidata)\\.org(:80)?', 3);

DROP TABLE IF EXISTS ellery.geo_pageviews_test;

CREATE  TABLE IF NOT EXISTS ellery.geo_pageviews_test (
  project STRING,
  variant STRING,
  page_title STRING,
  access_method STRING,
  country STRING);



INSERT OVERWRITE TABLE ellery.geo_pageviews_test
    SELECT
    get_project(uri_host) as project,
    get_variant(uri_host) as variant,
    REGEXP_EXTRACT(reflect("java.net.URLDecoder", "decode", uri_path), '^/[^/]*/(.*)', 1) as page_title,
    get_access_method(uri_host, user_agent) as access_method,
    geocode(resolve_ip(ip, x_forwarded_for)) as country
    FROM wmf.webrequest TABLESAMPLE(BUCKET 1 OUT OF 64 ON rand())
    WHERE year = 2015
    AND month = 1
    AND day = 20
    AND hour = 20
    AND uri_path NOT RLIKE '^/w/'
    AND is_pageview
    AND is_crawler(user_agent) = 0
    AND parse_ua(user_agent)['device_family'] != 'Spider';


SELECT * FROM ellery.geo_pageviews_test WHERE  access_method = 'mobile web' limit 10;


