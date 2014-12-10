

DROP TABLE IF EXISTS ellery.banner_count_%(year)d_%(month)d_%(day)d;

--This Is Really a Temporary Table need 0.14
CREATE TABLE ellery.banner_count_%(year)d_%(month)d_%(day)d(
  impressions_seen INT,
  banner STRING,
  campaign STRING,
  day STRING, 
  n INT
);

INSERT INTO TABLE ellery.banner_count_%(year)d_%(month)d_%(day)d
SELECT cast (a.banner_count as int) as impressions_seen, a.banner, a.campaign, a.day, count(*) as n
    FROM
    (SELECT
    PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'banner_count') as banner_count, 
    PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'banner') as banner,
    PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'campaign') as campaign, 
    substr(dt, 1, 10) as day
    FROM wmf_raw.webrequest 
    WHERE uri_path = '/wiki/Special:RecordImpression'
    AND PARSE_URL(CONCAT('http://bla.org/woo/', uri_query), 'QUERY', 'result') = 'show'
    AND year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
) a
GROUP BY a.banner_count, a.banner, a.campaign, a.day;
 

INSERT INTO TABLE ellery.banner_count
SELECT * FROM ellery.banner_count_%(year)d_%(month)d_%(day)d;

DROP TABLE ellery.banner_count_%(year)d_%(month)d_%(day)d;
