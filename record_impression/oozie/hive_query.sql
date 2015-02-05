set hive.stats.autogather=false;
USE ${database};

ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION parse_ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';


CREATE TABLE IF NOT EXISTS ellery.oozie_impressions_${version} (
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
)
PARTITIONED BY (year INT, month INT, day INT);

DROP TABLE IF EXISTS ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version};

--This Is Really a Temporary Table need 0.14
CREATE TABLE ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version} (
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

INSERT INTO TABLE ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version}

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
        WHEN parse_ua(user_agent)['device_family'] = 'Spider' THEN 1
        WHEN parse_ua(user_agent)['device_family'] != 'Other' THEN 0
        WHEN LOWER(user_agent) RLIKE 'bot|fetch|spider|slurp|crawler|sleuth' THEN 1
        WHEN LOWER(user_agent) RLIKE 'jakarta commons|tencenttraveler|genieo/|squider|gomezagent|quicklook|ning/|metauri api|daum|butterfly|guzzle|wada.vn|catchpoint' THEN 1
        WHEN LOWER(user_agent) RLIKE 'facebookexternalhit|pinterest|vkshare|flipboardproxy|twilioproxy' THEN 1
        WHEN LOWER(user_agent) RLIKE 'developers.google.com/+/web/snippet/|google web preview|bingpreview|sendgrid|secret_pin_test_agent|jumio callback clienti|paypal' THEN 1
        WHEN LOWER(user_agent) RLIKE 'libcurl|urllib|pycurl/|cpython|akamai_site_analyzer|monitis|check_http/v|chkd 1.2|node-xmlhttprequest|winhttp|sitecon' THEN 1
        ELSE 0
      END as spider

    FROM wmf.webrequest 
    
    WHERE uri_path = '/wiki/Special:RecordImpression'
      AND year = ${year}
      AND month = ${month}
      AND day = ${day}
      AND hour = ${hour}
      AND ((webrequest_source = 'text') OR (webrequest_source = 'mobile'))
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

 
INSERT INTO TABLE ellery.oozie_impressions_${version}
PARTITION (year = ${year}, month = ${month}, day = ${day})
SELECT * FROM ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version};

DROP TABLE ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version};
