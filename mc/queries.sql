
DROP TABLE IF EXISTS ellery.mc_dec;


CREATE TABLE IF NOT EXISTS ellery.mc_dec (
  prev_id INT, 
  curr_id INT,
  n BIGINT,
  prev_title STRING,
  curr_title STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


CREATE VIEW  main_pages AS
SELECT page_title, page_id
FROM en_page
WHERE page_namespace = 0;
--AND page_is_redirect = 0; 





INSERT OVERWRITE TABLE mc_dec
SELECT
eprev.page_id as prev_id,
ecurr.page_id as curr_id,
agg.n as n,
agg.prev as prev_title,
ecurr.page_title as curr_title
FROM
(SELECT prev, curr, SUM(n) as n
FROM ellery.oozie_mc
WHERE curr is NOT NULL
AND prev is NOT NULL
AND n is NOT NULL
AND year = 2014
AND month = 12
GROUP BY curr, prev
) agg
LEFT JOIN main_pages eprev
ON (eprev.page_title = agg.prev)
LEFT JOIN main_pages ecurr
ON (ecurr.page_title = agg.curr)
WHERE  ecurr.page_id is NOT NULL
AND (eprev.page_id is NOT NULL OR agg.prev in ('other-wikipedia', 'other-google', 'other-yahoo', 'other_empty', 'other'));

--interesting test queries
SELECT COUNT(*) from ellery.mc_agg; --72,503,849
SELECT SUM(n) from ellery.mc_agg; --780,436,905



SELECT COUNT(*) from ellery.mc_dec; --109,769,983
SELECT SUM(n) from ellery.mc_dec; --2,637,133,997

SELECT * from ellery.mc_dec
WHERE prev_title = 'Pig_farming'
ORDER BY n DESC
LIMIT 100;

SELECT * from ellery.mc_dec
WHERE curr_title = 'Germany'
ORDER BY n DESC
LIMIT 100;

SELECT * from ellery.mc_dec
WHERE prev_title = 'Skiing'
ORDER BY n DESC
LIMIT 2000;



--getting data out of hadoop
hadoop fs -copyToLocal /user/hive/warehouse/ellery.db/mc_dec mc_dec
cat mc_dec/* > mc_dec.txt
gzip mc_dec.txt






--create a view with only the top 5 links per page and add rank
DROP VIEW mc_agg_top5;

CREATE VIEW ellery.mc_agg_top5 AS
SELECT prev_title, curr_title, n, rank
FROM (
    SELECT prev_title,
           curr_title,
           ROW_NUMBER() over (PARTITION BY prev_title ORDER BY n DESC) as rank,
           n 
    FROM ellery.mc_agg
) ranked_mc_agg
WHERE ranked_mc_agg.rank <= 5;



--Un-flatten Recommendations

CREATE TABLE IF NOT EXISTS ellery.top_5_followed_links (
page STRING,
link1 STRING,
link2 STRING,
link3 STRING,
link4 STRING,
link5 STRING
);

ADD JAR /home/ellery/brickhouse-0.7.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';

INSERT OVERWRITE TABLE ellery.top_5_followed_links
SELECT prev_title as page,
top5_map[1] as link1,
top5_map[2] as link2,
top5_map[3] as link3,
top5_map[4] as link4,
top5_map[5] as link5

FROM (
SELECT prev_title, collect(rank, curr_title) top5_map
FROM ellery.mc_agg_top5
GROUP BY prev_title) a;


SELECT COUNT(*) FROM ellery.top_5_followed_links; --8,005,604
SELECT * FROM ellery.top_5_followed_links
WHERE page = 'Machine_learning';