ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION parse_ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';

CREATE TABLE ellery.record_impression_referers_test AS
SELECT 
banner, country, device, minute, project, reason, result, referer, count(*) as n
FROM
(SELECT 
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'banner') as banner,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'country') as country,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'device') as device,
      SUBSTR(dt, 0, 16) as minute,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'project') as project,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'reason') as reason,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'result') as result,
    CASE
      -- when prev has content
      WHEN parse_url(referer,'HOST') LIKE '%google.%' THEN 'other-google'
      WHEN parse_url(referer,'HOST') = 'en.wikipedia.org' AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
          THEN REGEXP_EXTRACT(parse_url(reflect("java.net.URLDecoder", "decode", referer),'PATH'), '/wiki/(.*)', 1)
      WHEN parse_url(referer,'HOST') LIKE '%.wikipedia.org%' THEN 'other-wikipedia'
      WHEN parse_url(referer,'HOST') RLIKE '\\.wiki.*\\.org' THEN 'other-internal'
      WHEN parse_url(referer,'HOST') LIKE '%yahoo.%' THEN 'other-yahoo'
      WHEN parse_url(referer,'HOST') LIKE '%facebook.%' THEN 'other-facebook'
      WHEN parse_url(referer,'HOST') LIKE '%twitter.%' THEN 'other-twitter'
      WHEN parse_url(referer,'HOST') LIKE '%t.co%' THEN 'other-twitter'
      WHEN parse_url(referer,'HOST') LIKE '%bing.%' THEN 'other-bing'
      WHEN parse_url(referer,'HOST') LIKE '%baidu.%' THEN 'other-baidu'
      WHEN referer == '' THEN 'other-empty'
      WHEN referer == '-' THEN 'other-empty'
      WHEN referer IS NULL THEN 'other-empty'
      ELSE 'other'
    END as referer
  FROM wmf_raw.webrequest
  --select the right partition
  WHERE webrequest_source in ('text', 'mobile')
    AND year = 2014
    AND month = 12
    AND day = 24
    AND hour = 2
    AND uri_path = '/wiki/Special:RecordImpression'
    AND parse_ua(user_agent)['device_family'] != 'Spider'
    AND  PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'campaign') ='C14_en5C_dec_dsk_FR') a
GROUP by banner, country, device, minute, project, reason, result, referer;
