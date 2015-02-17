
DROP TABLE IF EXISTS ellery.geo_pageviews_daily_test;

CREATE TABLE IF NOT EXISTS ellery.geo_pageviews_daily_test (
  project STRING,
  variant STRING,
  page_title STRING,
  access_method STRING,
  country STRING,
  n INT)
PARTITIONED BY (year INT, month INT, day INT);


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE ellery.geo_pageviews_daily_test
  PARTITION(year, month, day)
  SELECT  project, variant, page_title, access_method, country, n, year, month, day
  FROM ellery.geo_pageviews_daily
  WHERE year = 2015
  AND page_title = 'Main_Page'
  AND project = 'wikipedia'
  AND variant = 'en'
  AND country in ("US", "CA", "DE", "CN", "UK");

SELECT * FROM geo_pageviews_daily_test
WHERE year = 2015
ORDER BY country, access_method, year, month, day
LIMIT 100;


CREATE TABLE ellery.bursting (
  project STRING,
  variant STRING,
  page_title STRING,
  access_method STRING,
  country STRING,
  score FLOAT
);

rsync -rv ~/wmf/trending_locally/ stat1002.eqiad.wmnet:~/wmf/trending_locally
hadoop fs -mkdir /user/ellery/bursting
hadoop fs -mkdir /user/ellery/bursting/oozie

-- hadoop fs -rm -r /user/ellery/bursting/oozie/*
-- hadoop fs -put ~/wmf/trending_locally/bursting/oozie/* /user/ellery/bursting/oozie/


ADD FILE hdfs:///user/ellery/bursting/oozie/burst_scoring.py;


SELECT
  TRANSFORM( 
    x.project, 
    x.variant, 
    x.page_title,
    x.country,
    x.access_method,
    x.n
  ) USING 'python burst_scoring.py'
   AS 
    project, 
    variant, 
    page_title,
    country,
    access_method,
    score
FROM (
  SELECT 
    year, 
    month,
    day,
    project, 
    variant, 
    page_title,
    country,
    access_method,
    n
  FROM ellery.geo_pageviews_daily_test
  WHERE year = 2015
  DISTRIBUTE BY (
    project, 
    variant, 
    page_title,
    country,
    access_method
    )
  SORT BY project, variant, page_title, country, access_method, year, month, day ASC
) x;





INSERT INTO TABLE ellery.bursting_${year}_${month}_${day}_${version}

SELECT 
    curr.project,
    curr.variant,
    curr.page_title,
    curr.country,
    curr.n / prev.n as score
FROM

(SELECT
    SUM(n) as n,
    project,
    variant,
    page_title,
    country
FROM ellery.geo_pageviews_v0_1
WHERE year=2015
AND month=1
AND day=2
GROUP by project, variant, page_title, country) prev

JOIN

(SELECT
    SUM(n) as n,
    project,
    variant,
    page_title,
    country
FROM ellery.geo_pageviews_v0_1
WHERE year=2015
AND month=2
AND day=2
GROUP by project, variant, page_title, country) curr

ON(
    prev.project = curr.project
    AND prev.variant = curr.variant
    AND  prev.page_title = curr.page_title
    AND prev.country = curr.country)
;