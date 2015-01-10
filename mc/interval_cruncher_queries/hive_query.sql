
--this will be much easier once page_id is in x_analytics

CREATE TEMPORARY FUNCTION ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';

DROP TABLE IF EXISTS ellery.mc_%(year)d_%(month)d_%(day)d_%(hour)d;

CREATE TABLE ellery.mc_%(year)d_%(month)d_%(day)d_%(hour)d (
  year STRING,
  month STRING,
  day STRING,
  hour STRING,
  curr STRING,
  prev STRING,
  n BIGINT
);

INSERT INTO TABLE ellery.mc_%(year)d_%(month)d_%(day)d_%(hour)d

SELECT '%(year)d' as year, '%(month)d' as month, '%(day)d' as day, '%(hour)d' as hour, curr, prev, COUNT(*) n
FROM (
  SELECT 
    REGEXP_EXTRACT(uri_path, '/wiki/(.*)', 1) as curr,
    CASE
      WHEN parse_url(referer,'HOST') = 'en.wikipedia.org' AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1 THEN REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)
      WHEN parse_url(referer,'HOST') LIKE '%%.wikipedia.org%%' THEN 'other-wikipedia'
      WHEN parse_url(referer,'HOST') LIKE '%%.google%%' THEN 'other-google'
      WHEN referer NOT IN ('', '-') THEN 'other_empty'
      WHEN referer IS NULL THEN 'referer_empty'
      ELSE 'other'
    END as prev
  FROM ellery.pageviews_no_spiders x
  -- Filter out denial of serive attacks  
  JOIN (
    SELECT user_agent, ip, x_forwarded_for
    FROM ellery.pageviews_no_spiders
    WHERE year = %(year)d
      AND month = %(month)d
      AND day = %(day)d
      AND hour = %(hour)d
    GROUP BY user_agent, ip, x_forwarded_for
    HAVING COUNT(*) < 200
  ) y
  ON (x.ip = y.ip AND x.user_agent = y.user_agent AND x.x_forwarded_for = y.x_forwarded_for)
  WHERE uri_host = 'en.wikipedia.org'
    AND uri_path LIKE '/wiki/%%'
    -- Filter by right hour   
    AND year = %(year)d
    AND month = %(month)d
    AND day = %(day)d
    AND hour = %(hour)d
    -- Make sure curr page has content
    AND LENGTH(REGEXP_EXTRACT(uri_path, '/wiki/(.*)', 1)) > 0
    AND uri_path NOT LIKE '/wiki/Data:%%'
    AND uri_path NOT LIKE '/wiki/Talk:%%'
    AND uri_path NOT LIKE '/wiki/Wikipedia:%%'
    AND uri_path NOT LIKE '/wiki/File:%%'
    AND uri_path NOT LIKE '/wiki/Special:%%'
    -- Don't take prev pages that come from Data: or Special:
    AND (parse_url(referer,'HOST') != 'en.wikipedia.org' OR (
       parse_url(referer,'PATH') NOT LIKE '/wiki/Data:%%' AND parse_url(referer,'PATH') NOT LIKE '/wiki/Special:%%'))   
) a
WHERE curr != prev
  AND curr NOT IN ('undefined','Undefined')
GROUP BY curr, prev;


INSERT INTO TABLE ellery.mc
SELECT * FROM ellery.mc_%(year)d_%(month)d_%(day)d_%(hour)d;

DROP TABLE ellery.mc_%(year)d_%(month)d_%(day)d_%(hour)d;