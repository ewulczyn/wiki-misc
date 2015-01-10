

DROP TABLE IF EXISTS ellery.app_pageviews_%(year)d_%(month)d_%(day)d;

--This Is Really a Temporary Table need 0.14
CREATE TABLE ellery.app_pageviews_%(year)d_%(month)d_%(day)d(
  appInstallID STRING,
  uri_host STRING,
  page STRING
);

INSERT INTO TABLE ellery.app_pageviews_%(year)d_%(month)d_%(day)d
  SELECT 
  PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'appInstallID') as appInstallID,
  uri_host,
  PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'page') as page
  FROM wmf_raw.webrequest
  WHERE year = %(year)d
  AND month = %(month)d
  AND day = %(day)d
  AND uri_path = '/w/api.php'
  AND user_agent RLIKE 'WikipediaApp'
  AND uri_query RLIKE 'sections=0'
  AND PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'page') != 'Main+Page';
   

INSERT INTO TABLE ellery.app_pageviews
SELECT * FROM ellery.app_pageviews_%(year)d_%(month)d_%(day)d;

DROP TABLE ellery.app_pageviews_%(year)d_%(month)d_%(day)d;
