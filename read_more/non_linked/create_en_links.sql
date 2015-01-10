
DROP TABLE ellery.en_links_no_titles;

CREATE TABLE ellery.en_links_no_titles(
page_id INT,
link_id INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/ellery/read_more/non_linked/en_links_no_titles';


INSERT OVERWRITE TABLE ellery.en_links_no_titles
SELECT l.pl_from AS page_id, p.page_id AS link_id
FROM
ellery.en_page p JOIN ellery.en_pagelinks l
ON (l.pl_title = p.page_title AND l.pl_namespace = p.page_namespace)
WHERE p.page_namespace = 0
AND l.pl_from_namespace = 0
AND l.pl_namespace = 0
AND page_is_redirect = 0;


CREATE TABLE ellery.en_page_ids(
page_id INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/ellery/read_more/non_linked/en_page_ids';

INSERT OVERWRITE TABLE ellery.en_page_ids
SELECT DISTINCT(page_id) FROM ellery.en_links_no_titles;




fs -put ~/wmf/read_more/non_linked/ml_id.csv  /user/ellery/read_more/non_linked/


hadoop fs -rm -r -f temp/non_linked_recommendations
mahout recommenditembased \
--input /user/ellery/read_more/non_linked/en_links_no_titles \
--output /user/ellery/read_more/non_linked/all_non_linked_recommendations \
--numRecommendations 50  \
--usersFile  /user/ellery/read_more/non_linked/en_page_ids.txt \
--booleanData \
--similarityClassname SIMILARITY_LOGLIKELIHOOD \
--tempDir temp/non_linked_recommendations

SELECT  page_title from page where page_id in(4492598 ,250436 ,11731170 ,7872152 ,20399530 ,27379159 ,5797 ,25685 ,36548336 ,1451167)

