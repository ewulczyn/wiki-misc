
DROP TABLE IF EXISTS ellery.geo_pageviews_daily_test;

CREATE TABLE IF NOT EXISTS ellery.geo_pageviews_daily_test (
  project STRING,
  variant STRING,
  page_title STRING,
  country STRING,
  n INT)
PARTITIONED BY (year INT, month INT, day INT);


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE ellery.geo_pageviews_daily_test
  PARTITION(year, month, day)
  SELECT  project, variant, page_title, country, SUM(n), year, month, day
  FROM ellery.geo_pageviews_daily
  WHERE year = 2015
  AND page_title = 'Main_Page'
  AND project = 'wikipedia'
  AND variant = 'en'
  AND country in ("US", "CA", "DE", "CN", "UK")
  GROUP BY project, variant, page_title, country, year, month, day;

SELECT * FROM ellery.geo_pageviews_daily_test
WHERE year = 2015
ORDER BY country, year, month, day
LIMIT 100;



rsync -rv ~/wmf/trending_locally/ stat1002.eqiad.wmnet:~/wmf/trending_locally
hadoop fs -mkdir /user/ellery/trending
hadoop fs -mkdir /user/ellery/trending/oozie

-- hadoop fs -rm -r /user/ellery/trending/oozie/*
-- hadoop fs -put ~/wmf/trending_locally/trending/oozie/* /user/ellery/trending/oozie/

DROP TABLE IF EXISTS ellery.trending;
CREATE TABLE ellery.trending (
  project STRING,
  variant STRING,
  page_title STRING,
  country STRING,
  popularity FLOAT,
  trend FLOAT
);

ADD FILE hdfs:///user/ellery/trending/oozie/trend_scoring.py;

INSERT INTO TABLE ellery.trending
SELECT
  TRANSFORM( 
    x.project, 
    x.variant, 
    x.page_title,
    x.country,
    x.n
  ) USING 'python trend_scoring.py'
   AS 
    project, 
    variant, 
    page_title,
    country,
    popularity, 
    trend
FROM (
  SELECT 
    year, 
    month,
    day,
    project, 
    variant, 
    page_title,
    country,
    n
  FROM ellery.geo_pageviews_daily
  WHERE year = 2015
  AND project = 'wikipedia'

  DISTRIBUTE BY (
    project, 
    variant, 
    page_title,
    country
    )
  SORT BY project, variant, page_title, country, year, month, day ASC
) x;

SELECT * from ellery.trending
WHERE variant = 'en'
AND country = 'US'
AND project = 'wikipedia'
ORDER BY popularity DESC
LIMIT 100;
