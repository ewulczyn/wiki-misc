CREATE TEMPORARY FUNCTION ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';

--ALTER INDEX table10_index ON table10 PARTITION (columnX='valueQ', columnY='valueR') REBUILD

DROP TABLE IF EXISTS ellery.impressions_%(year)d_%(month)d_%(day)d_%(hour)d;

--This Is Really a Temporary Table need 0.14
CREATE TABLE ellery.impressions_%(year)d_%(month)d_%(day)d_%(hour)d (
  anonymous STRING,
  banner STRING,
  campaign STRING,
  country STRING,
  device STRING,
  minute STRING,
  project STRING,
  reason STRING,
  result STRING,
  uselang STRING,
  db STRING,
  bucket INT,
  spider BOOLEAN,
  n INT
);

INSERT INTO TABLE ellery.impressions_%(year)d_%(month)d_%(day)d_%(hour)d

SELECT 
  a.anonymous,
  a.banner,
  a.campaign,
  a.country,
  a.device,
  a.minute, 
  a.project, 
  a.reason,
  a.result,
  a.uselang,
  a.db,
  a.bucket,
  a.spider,
  COUNT(*) as n 
FROM (
    SELECT
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'anonymous') as anonymous,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'banner') as banner,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'campaign') as campaign,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'country') as country,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'device') as device,
      SUBSTR(dt, 0, 16) as minute,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'project') as project,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'reason') as reason,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'result') as result,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'uselang') as uselang,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'db') as db,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'bucket') as bucket,
      CASE 
        WHEN ua(user_agent)['device_family']= 'Spider' THEN 1
        WHEN ua(user_agent)['device_family'] != 'Other' THEN 0
        WHEN LOWER(user_agent) RLIKE 'bot|fetch|spider|slurp|crawler|sleuth' THEN 1
        WHEN LOWER(user_agent) RLIKE 'jakarta commons|tencenttraveler|genieo/|squider|gomezagent|quicklook|ning/|metauri api|daum|butterfly|guzzle|wada.vn|catchpoint' THEN 1
        WHEN LOWER(user_agent) RLIKE 'facebookexternalhit|pinterest|vkshare|flipboardproxy|twilioproxy' THEN 1
        WHEN LOWER(user_agent) RLIKE 'developers.google.com/+/web/snippet/|google web preview|bingpreview|sendgrid|secret_pin_test_agent|jumio callback clienti|paypal' THEN 1
        WHEN LOWER(user_agent) RLIKE 'libcurl|urllib|pycurl/|cpython|akamai_site_analyzer|monitis|check_http/v|chkd 1.2|node-xmlhttprequest|winhttp|sitecon' THEN 1
        ELSE 0
      END as spider

    FROM wmf_raw.webrequest 
    
    WHERE uri_path = '/wiki/Special:RecordImpression'
      AND year = %(year)d
      AND month = %(month)d
      AND day = %(day)d
      AND hour = %(hour)d
) a
GROUP BY
  a.anonymous,
  a.banner,
  a.campaign,
  a.country,
  a.device,
  a.minute, 
  a.project, 
  a.reason,
  a.result,
  a.uselang,
  a.db,
  a.bucket,
  a.spider;

 
INSERT INTO TABLE ellery.impressions
PARTITION (year = %(year)d, month = %(month)d, day = %(day)d)
SELECT * FROM ellery.impressions_%(year)d_%(month)d_%(day)d_%(hour)d;

DROP TABLE ellery.impressions_%(year)d_%(month)d_%(day)d_%(hour)d;
