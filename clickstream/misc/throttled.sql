USE ellery;
ADD FILE hdfs:///user/ellery/mc/oozie/throttle.py;
ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION parse_ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
CREATE TEMPORARY FUNCTION is_crawler as 'org.wikimedia.analytics.refinery.hive.IsCrawlerUDF';
CREATE TEMPORARY FUNCTION  resolve_ip as 'org.wikimedia.analytics.refinery.hive.ClientIpUDF';


CREATE VIEW relevant_mc_requests AS
    SELECT resolve_ip(ip, x_forwarded_for) as ip, user_agent, referer, SUBSTR(dt,1, 17) AS minute , SUBSTR(dt,18, 2) AS second, uri_path FROM wmf.webrequest
    WHERE webrequest_source = 'text'
    AND year = 2015
    AND month = 1
    AND day = 14
    AND hour =5
    -- only enwiki
    AND uri_host = 'en.wikipedia.org'

    -- make sure you get a human pageview
    AND (is_pageview OR
    -- or fish for redlinks
    (content_type in ('text/html\; charset=iso-8859-1',
                     'text/html\; charset=ISO-8859-1',
                     'text/html',
                     'text/html\; charset=utf-8',
                     'text/html\; charset=UTF-8') AND  http_status = '404' )
    )
    AND parse_ua(user_agent)['device_family'] != 'Spider'
    AND is_crawler(user_agent) = 0
    AND user_agent NOT RLIKE 'bot|Bot|spider|Spider|crawler|Crawler|http|Scraper|scraper'
    AND user_agent NOT RLIKE 'HTTrack|AppleDictionaryService|Twisted PageGetter|Akamai SureRoute|WikiWand|WordPress|MediaWiki'


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
    AND uri_path NOT LIKE '/wiki/Category:%'
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
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Category:%'
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
    AND PARSE_URL(referer, 'PATH') NOT LIKE '/wiki/Media:%'));

CREATE TABLE relevant_mc_requests_table (
    ip STRING,
    user_agent STRING,
    referer STRING,
    minute STRING,
    second STRING,
    uri_path STRING
);

INSERT OVERWRITE TABLE relevant_mc_requests_table
SELECT * from relevant_mc_requests;

SELECT * FROM relevant_mc_requests_table
WHERE referer like '%List_of_American_films_of_1982'
ORDER BY ip, user_agent, referer, minute, second
LIMIT 1000;

SELECT * from relevant_mc_requests
WHERE referer like '%List_of_Sri_Lankan_Tamils'
DISTRIBUTE BY (
    ip, 
    user_agent, 
    referer,
    minute
    )
SORT BY ip, user_agent, referer, minute, second
LIMIT 1000;


CREATE VIEW throttled_requests
AS SELECT TRANSFORM(
    x.ip, 
    x.user_agent, 
    x.referer,
    x.minute,
    x.second, 
    x.uri_path)
USING 'python throttle.py'
AS 
ip, 
user_agent, 
referer,
minute,
second, 
uri_path
FROM 
(SELECT
    ip, 
    user_agent, 
    referer,
    minute,
    second, 
    uri_path
FROM relevant_mc_requests_table
    DISTRIBUTE BY (
    ip, 
    user_agent, 
    referer,
    minute
    )
  SORT BY ip, user_agent, referer, minute, second) x;

drop table if exists ellery.mc_test;
create table ellery.mc_test(
prev STRING, 
curr STRING, 
n INT);

INSERT OVERWRITE TABLE ellery.mc_test
SELECT prev, curr, count(*) AS n
FROM (
  SELECT 
    REGEXP_EXTRACT(reflect("java.net.URLDecoder", "decode", uri_path), '/wiki/(.*)', 1) as curr,
    CASE
      WHEN parse_url(referer,'HOST') LIKE '%google.%' THEN 'other-google'
      -- when prev has content
      WHEN parse_url(referer,'HOST') = 'en.wikipedia.org' AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
          THEN reflect("java.net.URLDecoder", "decode", REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1))
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
    FROM  throttled_requests
) a
WHERE curr != prev
GROUP BY curr, prev;






SELECT curr, ratio FROM
(SELECT curr, CAST(out_n AS FlOAT) / CAST(in_n AS FLOAT) as ratio
FROM 
(SELECT prev, sum(n) as out_n from ellery.mc_test group by prev) out,
(SELECT curr, sum(n) as in_n from ellery.mc_test group by curr) in_t
where prev=curr) b
ORDER BY ratio desc limit 100;



select resolve_ip(ip, x_forwarded_for) as ip, user_agent, referer, SUBSTR(dt,1, 17) AS minute , SUBSTR(dt,18, 2) AS second, uri_path
from wmf.webrequest where year=2015 and month=1 and day=14 and hour = 5 and webrequest_source = 'text'
and referer like '%List_of_Sri_Lankan_Tamils' and uri_host = 'en.wikipedia.org' and is_pageview
order by ip, user_agent, referer, minute, second limit 1000;