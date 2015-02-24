
DROP TABLE IF EXISTS ellery.mc_jan;


CREATE TABLE IF NOT EXISTS ellery.mc_jan (
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
WHERE page_namespace = 0
AND page_is_redirect = 0; 



INSERT OVERWRITE TABLE mc_jan
  SELECT
  eprev.page_id as prev_id,
  ecurr.page_id as curr_id,
  agg.n as n,
  agg.prev as prev_title,
  ecurr.page_title as curr_title
  FROM
  (SELECT prev, curr, SUM(n) as n
  FROM ellery.oozie_mc_v0_5
  WHERE curr is NOT NULL
  AND prev is NOT NULL
  AND n is NOT NULL
  AND year = 2015
  AND month = 1
  GROUP BY curr, prev
  ) agg
  LEFT JOIN main_pages eprev
  ON (eprev.page_title = agg.prev)
  LEFT JOIN main_pages ecurr
  ON (ecurr.page_title = agg.curr)
  WHERE  ecurr.page_id is NOT NULL
  AND (eprev.page_id is NOT NULL OR agg.prev in ('other-wikipedia', 'other-google', 'other-yahoo', 'other-empty', 'other', 'other-facebook', 'other-twitter', 'other-bing', 'other-internal'))
  AND n > 10;


SELECT COUNT(*) from ellery.mc_jan;
 -- 21,998,518 
SELECT SUM(n) from ellery.mc_jan;
--4,043,692,615 



SELECT * from ellery.mc_jan
WHERE prev_id is NULL
ORDER BY n DESC
LIMIT 1000;


SELECT * from ellery.mc_jan
WHERE curr_id is NULL
ORDER BY n DESC
LIMIT 1000;

SELECT * from ellery.mc_jan
WHERE prev_title = 'Pig_farming'
ORDER BY n DESC
LIMIT 100;

SELECT * from ellery.mc_jan
WHERE prev_title = 'Germany'
ORDER BY n DESC
LIMIT 100;

SELECT * from ellery.mc_jan
WHERE prev_title = 'Skiing'
ORDER BY n DESC
LIMIT 100;

SELECT * from ellery.mc_jan
WHERE prev_title = 'other-twitter'
ORDER BY n DESC
LIMIT 100;

SELECT * from ellery.mc_jan
WHERE prev_title = 'other-facebook'
ORDER BY n DESC
LIMIT 100;

SELECT * from ellery.mc_dec
WHERE prev_title = 'other-empty'
ORDER BY n DESC
LIMIT 100;

SELECT * from ellery.mc_agg
WHERE prev_title = 'other-empty'
ORDER BY n DESC
LIMIT 100;


SELECT * from ellery.mc_jan
WHERE curr_title = 'other-baidu'
ORDER BY n DESC
LIMIT 100;

SELECT * from ellery.mc_jan
WHERE curr_title = 'Wikipedia'
ORDER BY n DESC
LIMIT 100;







