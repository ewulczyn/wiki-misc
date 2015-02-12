set hive.stats.autogather=false;
USE ${database};

ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION parse_ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';


CREATE TABLE IF NOT EXISTS ellery.oozie_mc_${version} (
  prev STRING,
  curr STRING,
  n BIGINT
)
PARTITIONED BY (year INT, month INT, day INT);



DROP TABLE IF EXISTS ellery.mc_${year}_${month}_${day}_${version};

CREATE TABLE ellery.mc_${year}_${month}_${day}_${version} (
  prev STRING,
  curr STRING,
  n BIGINT
);

INSERT INTO TABLE ellery.mc_${year}_${month}_${day}_${version}

SELECT prev, curr, COUNT(*) n
FROM (
  SELECT 
    REGEXP_EXTRACT(reflect("java.net.URLDecoder", "decode", uri_path), '/wiki/(.*)', 1) as curr,
    CASE
      -- when prev has content
      WHEN parse_url(referer,'HOST') LIKE '%google.%' THEN 'other-google'

      WHEN parse_url(referer,'HOST') = 'en.wikipedia.org' AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
          THEN REGEXP_EXTRACT(parse_url(reflect("java.net.URLDecoder", "decode", referer),'PATH'), '/wiki/(.*)', 1)
      WHEN parse_url(referer,'HOST') LIKE '%.wikipedia.org%' THEN 'other-wikipedia'
      WHEN parse_url(referer,'HOST') RLIKE '\\.wiki.*\\.org' THEN 'other-internal'
      WHEN parse_url(referer,'HOST') LIKE '%yahoo.%' THEN 'other-yahoo'
      WHEN parse_url(referer,'HOST') LIKE '%facebook.%' THEN 'other-facebook'
      WHEN parse_url(referer,'HOST') LIKE '%twitter.%' THEN 'other-twitter'
      WHEN parse_url(referer,'HOST') LIKE '%t.co%' THEN 'other-twitter'
      WHEN parse_url(referer,'HOST') LIKE '%bing.%' THEN 'other-bing'
      WHEN referer == '' THEN 'other-empty'
      WHEN referer == '-' THEN 'other-empty'
      WHEN referer IS NULL THEN 'other-empty'
      ELSE 'other'
    END as prev
  FROM wmf.webrequest
  --select the right partition
  WHERE webrequest_source = 'text'
    AND year = ${year}
    AND month = ${month}
    AND day = ${day}

    -- only enwiki
    AND uri_host = 'en.wikipedia.org'

    -- make sure you get a human pageview
    AND is_pageview
    AND parse_ua(user_agent)['device_family'] != 'Spider'
    
    --some media-wiki bug
    AND uri_path != '/wiki/Undefined'
    AND uri_path != '/wiki/undefined'

    -- Make sure curr page has content
    AND LENGTH(REGEXP_EXTRACT(uri_path, '/wiki/(.*)', 1)) > 0
    -- Try to only get articles in main namespace. MW craziness.
    AND uri_path NOT LIKE '/wiki/Talk:%' 
    AND uri_path NOT LIKE '/wiki/User:%' 
    AND uri_path NOT LIKE '/wiki/User_talk:%'
    AND uri_path NOT LIKE '/wiki/Wikipedia:%'
    AND uri_path NOT LIKE '/wiki/Wikipedia_talk:%'
    AND uri_path NOT LIKE '/wiki/File:%'
    AND uri_path NOT LIKE '/wiki/File_talk:%'
    AND uri_path NOT LIKE '/wiki/MediaWiki:%'
    AND uri_path NOT LIKE '/wiki/MediaWiki_talk:%'
    AND uri_path NOT LIKE '/wiki/Template:%'
    AND uri_path NOT LIKE '/wiki/Template_talk:%'
    AND uri_path NOT LIKE '/wiki/Help:%'
    AND uri_path NOT LIKE '/wiki/Help_talk:%'
    AND uri_path NOT LIKE '/wiki/Category_talk:%'
    AND uri_path NOT LIKE '/wiki/Portal:%'
    AND uri_path NOT LIKE '/wiki/Portal_talk:%'
    AND uri_path NOT LIKE '/wiki/Book:%'
    AND uri_path NOT LIKE '/wiki/Book_talk:%'
    AND uri_path NOT LIKE '/wiki/Draft:%'
    AND uri_path NOT LIKE '/wiki/Draft_talk:%'
    AND uri_path NOT LIKE '/wiki/EducationProgram:%'
    AND uri_path NOT LIKE '/wiki/EducationProgram_talk:%'
    AND uri_path NOT LIKE '/wiki/TimedText:%'
    AND uri_path NOT LIKE '/wiki/TimedText_talk:%'
    AND uri_path NOT LIKE '/wiki/Module:%'
    AND uri_path NOT LIKE '/wiki/Module_talk:%'
    AND uri_path NOT LIKE '/wiki/Topic:%'
    AND uri_path NOT LIKE '/wiki/Topic_talk:%'
    AND uri_path NOT LIKE '/wiki/Data:%'
    AND uri_path NOT LIKE '/wiki/Special:%'
    AND uri_path NOT LIKE '/wiki/Media:%'



    AND (PARSE_URL(referer, 'PATH') is NULL
    OR (PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Talk:%' 
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/User:%' 
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/User_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Wikipedia:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Wikipedia_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/File:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/File_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/MediaWiki:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/MediaWiki_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Template:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Template_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Help:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Help_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Category_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Portal:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Portal_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Book:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Book_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Draft:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Draft_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/EducationProgram:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/EducationProgram_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/TimedText:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/TimedText_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Module:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Module_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Topic:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Topic_talk:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Data:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Special:%'
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Media:%'))

) a
WHERE curr != prev
GROUP BY curr, prev;

INSERT INTO TABLE ellery.oozie_mc_${version}
PARTITION (year = ${year}, month = ${month}, day = ${day})
SELECT * FROM ellery.mc_${year}_${month}_${day}_${version};
DROP TABLE ellery.mc_${year}_${month}_${day}_${version};