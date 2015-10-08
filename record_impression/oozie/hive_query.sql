set hive.stats.autogather=false;
USE ${database};


CREATE TABLE IF NOT EXISTS ellery.oozie_impressions_${version} (
  banner STRING,
  minute STRING,
  n INT
)
PARTITIONED BY (year INT, month INT, day INT);


DROP TABLE IF EXISTS ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version};

--This Is Really a Temporary Table need 0.14
CREATE TABLE  ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version} (
  banner STRING,
  minute STRING,
  n INT
);

INSERT INTO TABLE ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version}

SELECT 
  a.banner,
  a.minute, 
  COUNT(*) as n 
FROM (
    SELECT
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'banner') as banner,
      SUBSTR(dt, 0, 16) as minute
    FROM wmf.webrequest 
    WHERE year = ${year}
      AND month = ${month}
      AND day = ${day}
      AND hour = ${hour}
      AND ((webrequest_source = 'text') OR (webrequest_source = 'mobile'))
      AND uri_host in ('meta.wikimedia.org', 'meta.m.wikimedia.org')
      AND uri_path = '/w/index.php'
      AND PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'title') IS NOT NULL
      AND PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'title') = 'Special:BannerLoader'
      AND agent_type = 'user'

) a
GROUP BY
  a.banner,
  a.minute;

 
INSERT INTO TABLE ellery.oozie_impressions_${version}
PARTITION (year = ${year}, month = ${month}, day = ${day})
SELECT * FROM ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version};

DROP TABLE ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version};
