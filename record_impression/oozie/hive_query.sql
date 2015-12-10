set hive.stats.autogather=false;
USE ${database};


CREATE TABLE IF NOT EXISTS ellery.oozie_impressions_${version} (
  banner STRING,
  campaign STRING,
  referer STRING,
  country STRING,
  dt STRING,
  n INT
)
PARTITIONED BY (year INT, month INT, day INT);


DROP TABLE IF EXISTS ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version};

--This Is Really a Temporary Table need 0.14
CREATE TABLE  ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version} (
  banner STRING,
  campaign STRING,
  referer STRING,
  country STRING,
  dt STRING,
  n INT
);

INSERT INTO TABLE ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version}
SELECT 
  a.banner,
  a.campaign,
  a.referer,
  a.country,
  a.dt,
  COUNT(*) as n 
FROM (
    SELECT
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'banner') as banner,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'campaign') as campaign,
      referer,
      PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'country') as country,
      concat(year,'-',month,'-',day, ' ', hour, ':00') as dt
    FROM wmf.webrequest 
    WHERE year = ${year}
      AND month = ${month}
      AND day = ${day}
      AND hour = ${hour}
      AND ((webrequest_source = 'text') OR (webrequest_source = 'mobile'))
      AND uri_path = '/beacon/impression'
      AND PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'result') = 'show'
      AND agent_type = 'user'
) a
GROUP BY
  a.banner,
  a.campaign,
  a.referer,
  a.country,
  a.dt;

 
INSERT INTO TABLE ellery.oozie_impressions_${version}
PARTITION (year = ${year}, month = ${month}, day = ${day})
SELECT * FROM ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version};

DROP TABLE ellery.oozie_impressions_${year}_${month}_${day}_${hour}_${version};
