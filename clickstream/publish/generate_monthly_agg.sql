export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64

-- sqoop page, pagelink, redirect tables into HIVE

sqoop import                                                        \
  --connect jdbc:mysql://s1-analytics-slave.eqiad.wmnet/enwiki      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_XXXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password XXXXXXXXXXXX                                           \
  --split-by a.page_id                                              \
  --hive-import                                                     \
  --hive-database ellery                                            \
  --create-hive-table                                               \
  --hive-table en_page                                              \
  --query '
SELECT
  a.page_id AS page_id,
  CAST(a.page_title AS CHAR(255) CHARSET utf8) AS page_title
FROM page a
WHERE $CONDITIONS AND page_namespace = 0
'                                          


sqoop import                                                        \
  --connect jdbc:mysql://s1-analytics-slave.eqiad.wmnet/enwiki      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_1XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password XXXXXXXXXXXX                                           \
  --split-by b.rd_from                                              \
  --hive-import                                                     \
  --hive-database ellery                                            \
  --create-hive-table                                               \
  --hive-table en_redirect                                          \
  --query '
SELECT
  b.rd_from AS rd_from,
  CAST(b.rd_title AS CHAR(255) CHARSET utf8) AS rd_title
FROM redirect b
WHERE $CONDITIONS AND rd_namespace = 0
'                 

sqoop import                                                        \
  --connect jdbc:mysql://s1-analytics-slave.eqiad.wmnet/enwiki      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_2XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password XXXXXXXXXXXX                                           \
  --split-by a.pl_from                                              \
  --hive-import                                                     \
  --hive-database ellery                                            \
  --create-hive-table                                               \
  --hive-table en_pagelinks                                          \
  --query '
SELECT
  a.pl_from AS pl_from,
  CAST(a.pl_title AS CHAR(255) CHARSET utf8) AS pl_title
FROM pagelinks a
WHERE pl_namespace = 0
AND pl_from_namespace = 0
AND $CONDITIONS

-- create view of redirect table with ids -> titles           
CREATE VIEW redirect AS
SELECT 
en_page.page_title as r_from,
en_redirect.rd_title as r_to
FROM
en_redirect INNER JOIN en_page
ON en_redirect.rd_from = en_page.page_id;

-- create view of pagrlinks table with ids -> titles           
CREATE VIEW pagelinks AS
SELECT
en_page.page_title AS l_from,
en_pagelinks.pl_title AS l_to
FROM en_pagelinks INNER JOIN en_page
ON en_pagelinks.pl_from = en_page.page_id;


-- aggregate daily clickstream 
CREATE VIEW agg AS
SELECT prev, curr, SUM(n) as n
FROM ellery.oozie_mc_v0_5
WHERE curr is NOT NULL
AND prev is NOT NULL
AND n is NOT NULL
AND year = 2015
AND month = 1
GROUP BY curr, prev;




-- resolve redirects
CREATE VIEW agg_redirect_raw AS
SELECT
CASE
    WHEN prev in ('other-wikipedia', 'other-google', 'other-yahoo', 'other-empty', 'other', 'other-facebook', 'other-twitter', 'other-bing', 'other-internal') THEN prev
    WHEN redirect_prev.r_to IS NOT NULL THEN redirect_prev.r_to
    ELSE prev
END AS prev,
CASE
    WHEN redirect_curr.r_to IS NOT NULL THEN redirect_curr.r_to
    ELSE curr
END AS curr, 
agg.n as n
FROM agg LEFT JOIN redirect redirect_curr
ON (agg.curr = redirect_curr.r_from)
LEFT JOIN redirect redirect_prev
ON (agg.prev = redirect_prev.r_from);


-- reaggregate after resolving redirects
DROP VIEW IF EXISTS agg_redirect;
CREATE VIEW agg_redirect AS
SELECT prev, curr, SUM(n) as n
FROM agg_redirect_raw
GROUP BY prev, curr;


-- mark prev, curr as a link to capture redlinks
DROP VIEW IF EXISTS agg_links;
CREATE VIEW agg_links AS
SELECT
agg_redirect.prev AS prev,
agg_redirect.curr AS curr,
agg_redirect.n AS n,
pagelinks.l_from IS NOT NULL AND pagelinks.l_to is NOT NULL AS is_link
FROM agg_redirect LEFT JOIN pagelinks
ON (agg_redirect.prev = pagelinks.l_from AND agg_redirect.curr = pagelinks.l_to);


-- mark prev, curr as pages
CREATE VIEW agg_id AS
SELECT
prev_pages.page_id AS prev_id,
curr_pages.page_id curr_id,
agg.n AS n,
agg.prev AS prev,
agg.curr AS curr,
agg.is_link AS is_link
FROM agg_links agg
LEFT JOIN en_page prev_pages
ON (prev_pages.page_title = agg.prev)
LEFT JOIN en_page curr_pages
ON (curr_pages.page_title = agg.curr);

--IF REDOING, REPLACE OTHER with  ADD OTHER-OTHER !!!!!!!!!!!!!!!!!!
-- filter down to legal pairs
CREATE VIEW agg_filtered AS
SELECT prev_id, curr_id, n, prev, curr, 
CASE
    WHEN is_link AND prev_id IS NOT NULL AND curr_id IS NOT NULL THEN 'link'
    WHEN is_link AND prev_id IS NOT NULL AND curr_id IS NULL THEN 'redlink'
    WHEN is_link THEN 'other-link'
    ELSE 'other'
END type
FROM agg_id
WHERE (prev_id IS NOT NULL OR prev in ('other-wikipedia', 'other-google', 'other-yahoo', 'other-empty', 'other', 'other-facebook', 'other-twitter', 'other-bing', 'other-internal'))
AND (curr_id IS NOT NULL OR is_link);


DROP TABLE IF EXISTS mc_jan;
CREATE TABLE IF NOT EXISTS mc_jan (
  prev_id INT,
  curr_id INT,
  n BIGINT,
  prev STRING,
  curr STRING,
  type STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE mc_jan
SELECT * FROM agg_filtered
WHERE n >= 10;




