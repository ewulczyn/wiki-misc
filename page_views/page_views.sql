

--translation of https://github.com/Ironholds/WMUtils/blob/master/R/log_sieve.R to hive

CREATE VIEW ellery.pageviews AS
SELECT  * FROM wmf_raw.webrequest 
--MIME level filtering: a bug in hive requires escaping the semicolon
WHERE content_type in ('text/html\; charset=iso-8859-1',
                       'text/html\; charset=ISO-8859-1',
                       'text/html',
                       'text/html\; charset=utf-8',
                       'text/html\; charset=UTF-8',
                       'application/json\; charset=utf-8')
--Limit to 'production' sites                       
AND uri_host RLIKE  '((commons|meta|species)\\.((m|mobile|wap|zero)\\.)?wikimedia\\.)|(wik(tionary|isource|ibooks|ivoyage|iversity|iquote|inews|ipedia|idata)\\.)'
--Exclude non-app API hits 
AND (uri_path NOT RLIKE 'api.php' OR ( user_agent RLIKE 'WikipediaApp' AND CONCAT(uri_host, CONCAT(uri_path, uri_query)) RLIKE 'sections=0' ))
--Limit to content directories
AND CONCAT(uri_path, uri_query)  RLIKE '(/zh(-(tw|cn|hant|mo|my|hans|hk|sg))?/|/sr(-(ec|el))?/|/wiki(/|\\?(cur|old)id=)|/w/|/\\?title=)'
--Limit to successful requests
AND http_status = '200'
--Exclude internal requests
AND user_agent NOT RLIKE '^MediaWiki/1\\.'
--Exclude special pages and searches
AND uri_path NOT RLIKE 'Special:'
AND CONCAT(uri_path, uri_query)  NOT RLIKE 'index.php\\?search';


--now we might want to add spider removal, bing and google bots rule wikipedia :)

CREATE TEMPORARY FUNCTION ua as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';

CREATE VIEW ellery.pageviews_no_spiders AS
SELECT  * FROM ellery.pageviews
WHERE ua(user_agent)['device_family'] != 'Spider';


--now we want to remove people with more than 200 pageviews per hour, you have to do that spereratley through a join


SELECT *
   
  FROM ellery.pageviews_no_spiders x
  JOIN (
    SELECT user_agent, ip
    FROM ellery.pageviews_no_spiders
    WHERE year = 2014
      AND month = 11
      AND day = 21
      AND hour = 10
    GROUP BY user_agent, ip
    HAVING COUNT(*) < 200
  ) y
  ON (x.ip = y.ip AND x.user_agent = y.user_agent)
  WHERE year = 2014
      AND month = 11
      AND day = 21
      AND hour = 10;
