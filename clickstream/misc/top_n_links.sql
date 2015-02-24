
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