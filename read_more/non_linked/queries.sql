--/user/hive/warehouse/ellery.db/mc_agg

--/user/ellery/

-- generate pages with common in links
mahout itemsimilarity \
-i /user/hive/warehouse/ellery.db/mc_agg \
-o /user/hive/warehouse/ellery.db/page_sim_in_links \
--tempDir temp/page_sim_in_links \
-s SIMILARITY_TANIMOTO_COEFFICIENT \
--maxSimilaritiesPerItem 1000 \
-b



CREATE EXTERNAL TABLE IF NOT EXISTS ellery.page_sim_in_links (
    page_id INT,
    link_id INT,
    score FLOAT
 )
LOCATION '/user/hive/warehouse/ellery.db/page_sim_in_links';




--translate ids to strings
INSERT OVERWRITE TABLE common_in_links_str
SELECT /*+ STREAMTABLE(common_in_links) */ page, link, score
FROM 
common_in_links JOIN page_to_id ON (common_in_links.page_id = page_to_id.page_id)
JOIN link_to_id on (common_in_links.link_id = link_to_id.link_id)





DROP TABLE IF EXISTS  ellery.in_links;

CREATE TABLE IF NOT EXISTS ellery.in_links (
  page_id INT,
  link_id INT,
  n INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';


INSERT OVERWRITE TABLE ellery.in_links
    SELECT HASH(curr) as page_id, 
    HASH(prev) as link_id,
    n
    FROM ellery.mc_agg
    WHERE prev IS NOT NULL
    AND curr IS NOT NULL
    AND n IS NOT NULL;

