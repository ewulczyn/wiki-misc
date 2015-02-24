set hive.stats.autogather=false;
set hive.support.concurrency=false;
ADD FILE hdfs:///user/ellery/trending/oozie/trend_scoring.py;

USE ${database};


CREATE TABLE IF NOT EXISTS ellery.${task} (
  project STRING,
  variant STRING,
  page_title STRING,
  country STRING,
  popularity FLOAT,
  trend FLOAT)
PARTITIONED BY (year INT, month INT, day INT);




DROP TABLE IF EXISTS ellery.${task}_${year}_${month}_${day};

CREATE TABLE ellery.${task}_${year}_${month}_${day} (
  project STRING,
  variant STRING,
  page_title STRING,
  country STRING,
  popularity FLOAT,
  trend FLOAT
);

INSERT INTO TABLE ellery.${task}_${year}_${month}_${day}
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
  WHERE year >= 2015
  AND project = 'wikipedia'
  DISTRIBUTE BY (
    project, 
    variant, 
    page_title,
    country
    )
  SORT BY project, variant, page_title, country, year, month, day ASC
) x;



INSERT INTO TABLE ellery.${task}
PARTITION (year = ${year}, month = ${month}, day = ${day})
SELECT * FROM ellery.${task}_${year}_${month}_${day};
DROP TABLE ellery.${task}_${year}_${month}_${day};
ALTER TABLE ${task} DROP IF EXISTS PARTITION(year = ${last_week_year}, month = ${last_week_month}, day = ${last_week_day});

