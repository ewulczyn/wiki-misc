

DROP TABLE IF EXISTS ellery.mahout_app_pageviews;

CREATE TABLE IF NOT EXISTS ellery.mahout_app_pageviews (
  integer_appInstallID INT,
  page_id INT,
  n INT,
  appInstallID STRING,
  page_title STRING
);


INSERT OVERWRITE TABLE mahout_app_pageviews
    SELECT HASH(pv.appInstallID) as integer_appInstallID, 
    encoded_pages.page_id as page_id,
    pv.n,
    pv.appInstallID as appInstallID,
    encoded_pages.page_title as page_title
    FROM 
    (
    SELECT appInstallID, page, COUNT(*) as n
    FROM app_pageviews
    WHERE uri_host = 'en.m.wikipedia.org'
    GROUP BY appInstallID, page
    ) pv JOIN encoded_pages
    ON (pv.page = encoded_pages.encoded_page_title);
    
    


CREATE TABLE IF NOT EXISTS ellery.user_id_map (
  appInstallID STRING,
  user_id INT,
);


INSERT OVERWRITE INTO ellery.user_id_map
    SELECT appInstallID, hash(appInstallID) as user_id
    FROM ellery.mahout_app_pageviews
    GROUP BY appInstallID;



mahout recommenditembased \
--input /user/hive/warehouse/ellery.db/mahout_app_pageviews \
--output /user/hive/warehouse/ellery.db/app_recommendations \
--numRecommendations 10  \
--usersFile items.csv \
--booleanData \
--similarityClassname SIMILARITY_TANIMOTO_COEFFICIENT \
--tempDir temp/app_recommendations

