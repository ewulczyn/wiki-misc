set hive.stats.autogather=false;
USE ${database};

ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION parse_ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';



DROP TABLE IF EXISTS ellery.mc_${year}_${month}_${day};

CREATE TABLE ellery.mc_${year}_${month}_${day} (
  prev STRING,
  curr STRING,
  n BIGINT
);

INSERT INTO TABLE ellery.mc_${year}_${month}_${day}

SELECT prev, curr, COUNT(*) n
FROM (
  SELECT 
    REGEXP_EXTRACT(reflect("java.net.URLDecoder", "decode", uri_path), '/wiki/(.*)', 1) as curr,
    CASE
      -- when prev has content
      WHEN parse_url(referer,'HOST') = 'en.wikipedia.org' AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
          THEN REGEXP_EXTRACT(parse_url(reflect("java.net.URLDecoder", "decode", referer),'PATH'), '/wiki/(.*)', 1)
      WHEN parse_url(referer,'HOST') LIKE '%.wikipedia.org%' THEN 'other-wikipedia'
      WHEN parse_url(referer,'HOST') LIKE '%.google%' THEN 'other-google'
      WHEN parse_url(referer,'HOST') LIKE '%.yahoo%' THEN 'other-yahoo'
      WHEN referer NOT IN ('', '-') THEN 'other_empty'
      WHEN referer IS NULL THEN 'other_empty'
      ELSE 'other'
    END as prev
  FROM wmf_raw.webrequest
  --select the right partition
  WHERE webrequest_source = 'text'
    AND year = ${year}
    AND month = ${month}
    AND day = ${day}

    -- only enwiki
    AND uri_host = 'en.wikipedia.org'

    -- make sure you get a human pageview
    AND parse_ua(user_agent)['device_family'] != 'Spider'
    AND uri_path LIKE '/wiki/%'
    AND uri_path NOT RLIKE 'api.php'
    AND http_status = '200'
    AND user_agent NOT RLIKE '^MediaWiki/1\\.'
    AND CONCAT(uri_path, uri_query)  NOT RLIKE 'index.php\\?search'
    AND content_type in ('text/html\; charset=iso-8859-1',
                       'text/html\; charset=ISO-8859-1',
                       'text/html',
                       'text/html\; charset=utf-8',
                       'text/html\; charset=UTF-8',
                       'application/json\; charset=utf-8')
    --some media-wiki bug
    AND uri_path != '/wiki/Undefined'
    AND uri_path != '/wiki/undefined'

    -- Make sure curr page has content
    AND LENGTH(REGEXP_EXTRACT(uri_path, '/wiki/(.*)', 1)) > 0
    -- Try to only get articles in main namespace
    AND uri_path NOT LIKE '/wiki/Data:%'
    AND uri_path NOT LIKE '/wiki/Talk:%'
    AND uri_path NOT LIKE '/wiki/Wikipedia:%'
    AND uri_path NOT LIKE '/wiki/File:%'
    AND uri_path NOT LIKE '/wiki/Portal:%'
    AND uri_path NOT LIKE '/wiki/Category:%'
    AND uri_path NOT LIKE '/wiki/Template:%'
    AND uri_path NOT LIKE '/wiki/Help:%'
    AND uri_path NOT LIKE '/wiki/Book:%'
    AND uri_path NOT LIKE '/wiki/Draft:%'
    AND uri_path NOT LIKE '/wiki/Module:%'
    AND uri_path NOT LIKE '/wiki/Topic:%'
    AND uri_path NOT LIKE '/wiki/User:%'
    AND uri_path NOT LIKE '/wiki/Special:%'
    AND uri_path NOT LIKE '/wiki/%_talk:%'

    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Data:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Talk:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Wikipedia:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/File:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Portal:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Category:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Template:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Help:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Book:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Draft:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Module:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Topic:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/User:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/Special:%'
    AND PARSE_URL(referer,'PATH') NOT LIKE '/wiki/%_talk:%'
) a
WHERE curr != prev
GROUP BY curr, prev;


INSERT INTO TABLE ellery.oozie_mc
PARTITION (year = ${year}, month = ${month}, day = ${day})
SELECT * FROM ellery.mc_${year}_${month}_${day};

DROP TABLE ellery.mc_${year}_${month}_${day};