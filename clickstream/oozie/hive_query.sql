set hive.stats.autogather=false;
USE ${database};

-- UDFS and TRANSFORM functions
ADD FILE hdfs:///user/ellery/clickstream/oozie/throttle.py;
ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION parse_ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
CREATE TEMPORARY FUNCTION is_crawler as 'org.wikimedia.analytics.refinery.hive.IsCrawlerUDF';
CREATE TEMPORARY FUNCTION  resolve_ip as 'org.wikimedia.analytics.refinery.hive.ClientIpUDF';

-- create a table that will contain the daily clickstream data
CREATE TABLE IF NOT EXISTS ellery.clickstream_${version} (
  prev STRING,
  curr STRING,
  n BIGINT
)
PARTITIONED BY (year INT, month INT, day INT);


-- create a view that corresponds to requests that are relevant for the clickstream data set
-- we include pageviews of articles in the main namespace of enwiki
DROP VIEW IF EXISTS ellery.relevant_requests_${year}_${month}_${day}_${version};
CREATE VIEW ellery.relevant_requests_${year}_${month}_${day}_${version} AS
    SELECT resolve_ip(ip, x_forwarded_for) as ip,
    user_agent,
    referer, SUBSTR(dt,1, 17) AS minute,
    SUBSTR(dt, 18) AS second,
    uri_path
    FROM wmf.webrequest
    WHERE webrequest_source = 'text'
    AND year = ${year}
    AND month = ${month}
    AND day = ${day}
    -- only requests for enwiki
    AND uri_host = 'en.wikipedia.org'
    -- make sure you get a pageview
    AND (is_pageview OR
    -- or fish for redlinks
    (content_type in ('text/html\; charset=iso-8859-1',
                     'text/html\; charset=ISO-8859-1',
                     'text/html',
                     'text/html\; charset=utf-8',
                     'text/html\; charset=UTF-8') AND  http_status = '404' )
    )
    -- remove bots/spiders that signal their identity through their user-agent
    AND parse_ua(user_agent)['device_family'] != 'Spider'
    AND is_crawler(user_agent) = 0
    AND user_agent NOT RLIKE 'bot|Bot|spider|Spider|crawler|Crawler|http|Scraper|scraper'
    AND user_agent NOT RLIKE 'HTTrack|AppleDictionaryService|Twisted PageGetter|Akamai SureRoute|WikiWand|WordPress|MediaWiki'


    --some media-wiki bug
    AND uri_path != '/wiki/Undefined'
    AND uri_path != '/wiki/undefined'

    -- Make sure curr page has content
    AND LENGTH(REGEXP_EXTRACT(uri_path, '/wiki/(.*)', 1)) > 0
    -- Try to only get requests for articles in the main namespace.
    -- This is MW craziness. There is no single regex to find
    -- articles in the main namespace because colons are legal in
    -- main namespace article titles
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
    AND uri_path NOT LIKE '/wiki/Media:%';


-- create a view corresponding to relvant requests where requests from crawlers are exluded
-- throttle.py does this by rate limiting requests from the same client with the same referer 
DROP VIEW IF EXISTS ellery.throttled_requests_${year}_${month}_${day}_${version};

CREATE VIEW ellery.throttled_requests_${year}_${month}_${day}_${version}
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
    FROM ellery.relevant_requests_${year}_${month}_${day}_${version}
    DISTRIBUTE BY (
        ip, 
        user_agent, 
        referer,
        minute
        )
    SORT BY ip, user_agent, referer, minute, second) x;


-- create a "temporary" table to hold the days worth of clickstream data
-- our version of hive does not support temporary tables so we mimic them
-- this is necessary because, otherwise the main clickstream table
-- is locked while we process the next days worth of data
DROP TABLE IF EXISTS ellery.clickstream_${year}_${month}_${day}_${version};
CREATE TABLE ellery.clickstream_${year}_${month}_${day}_${version} (
  prev STRING,
  curr STRING,
  n BIGINT
);

-- finally map referers to a set of predefined categories
-- and insert into the "temporary" table
INSERT INTO TABLE ellery.clickstream_${year}_${month}_${day}_${version}
SELECT prev, curr, count(*) AS n
FROM (
  SELECT 
    REGEXP_EXTRACT(reflect("java.net.URLDecoder", "decode", uri_path), '/wiki/(.*)', 1) as curr,
    CASE
        WHEN referer == '' THEN 'other-empty'
        WHEN referer == '-' THEN 'other-empty'
        WHEN referer IS NULL THEN 'other-empty'
        WHEN PARSE_URL(referer, 'PATH') is NULL THEN 'other'
        WHEN parse_url(referer,'HOST') = 'en.wikipedia.org'
            AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
            AND (PARSE_URL(referer, 'PATH')  LIKE '/wiki/Talk:%' 
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/User:%' 
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/User_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Wikipedia:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Wikipedia_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/File:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/File_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/MediaWiki:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/MediaWiki_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Template:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Template_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Help:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Help_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Category:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Category_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Portal:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Portal_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Book:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Book_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Draft:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Draft_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/EducationProgram:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/EducationProgram_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/TimedText:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/TimedText_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Module:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Module_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Topic:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Topic_talk:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Data:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Special:%'
                OR PARSE_URL(referer, 'PATH') LIKE '/wiki/Media:%')
            THEN 'other-wikipedia'
        WHEN parse_url(referer,'HOST') = 'en.wikipedia.org' AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
            THEN reflect("java.net.URLDecoder", "decode", REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1))
        WHEN parse_url(referer,'HOST') LIKE '%.wikipedia.org%' THEN 'other-wikipedia'
        WHEN parse_url(referer,'HOST') RLIKE '\\.wiki.*\\.org' THEN 'other-internal'
        WHEN parse_url(referer,'HOST') LIKE '%google.%' THEN 'other-google'
        WHEN parse_url(referer,'HOST') LIKE '%yahoo.%' THEN 'other-yahoo'
        WHEN parse_url(referer,'HOST') LIKE '%facebook.%' THEN 'other-facebook'
        WHEN parse_url(referer,'HOST') LIKE '%twitter.%' THEN 'other-twitter'
        WHEN parse_url(referer,'HOST') LIKE '%t.co%' THEN 'other-twitter'
        WHEN parse_url(referer,'HOST') LIKE '%bing.%' THEN 'other-bing'
        ELSE 'other-other'
    END as prev
    FROM  ellery.throttled_requests_${year}_${month}_${day}_${version}
) a
WHERE curr != prev
GROUP BY curr, prev;

-- transfer data from the temporary table to the final table
INSERT INTO TABLE ellery.clickstream_${version}
PARTITION (year = ${year}, month = ${month}, day = ${day})
SELECT * FROM ellery.clickstream_${year}_${month}_${day}_${version};

-- drop temporary, day specific views and tables
DROP TABLE ellery.clickstream_${year}_${month}_${day}_${version};
DROP VIEW IF EXISTS ellery.throttled_requests_${year}_${month}_${day}_${version};
DROP VIEW IF EXISTS ellery.relevant_requests_${year}_${month}_${day}_${version};

