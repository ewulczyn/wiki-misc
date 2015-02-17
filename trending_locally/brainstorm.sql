
###Tasks
- define trending
- score each artitle time-series by how trnding it is currenlty (trend prediction is another question)
- find most trending articles in a set of articles (e.g. enwiki, sports on enwiki) 

###What is "trending"?

- the article currently has many pageviews
- the articles recently saw a boost in pageviews

### Oozie
make geo pageviews daily and write out success flag
make oozie job flow for trending, 
    hive table with trend scores
    SQL table with scores for current day




ALG1:


def calc_monthly_trend(dates, pageviews):
  dts,counts = zip( *sorted( zip (dates,pageviews)))
  trend_2 = sum(counts[-15:])
  trend_1 = 1.0*sum(counts[-30:-15])
  monthly_trend = trend_2 - trend_1
  return monthly_trend
  
def calc_daily_trend(dates, pageviews, total_pageviews):
  '''
  Dead simple trend algorithm used for demo
  Only needs the last 10 days of data
  '''
  # ~Today's pageviews...
  y2 = pageviews[-1]
  # ~Yesterdays pageviews...
  y1 = pageviews[-8]
  # ~Significance factor based on previous week's pageviews
  weekly_pageviews = sum(pageviews[-8:-1])  
  # Simple baseline trend algorithm
  slope = y2 - y1
  trend = slope  * (1.0 + log(1.0 +int(weekly_pageviews)))
  error = 1.0/sqrt(int(total_pageviews))  
  return trend, error  



compute daily pageviews 30 days ago and today, score is the ratio

DROP TABLE ellery.trending_alg1;
CREATE TABLE IF NOT EXISTS ellery.trending_alg1 (
  project STRING,
  variant STRING,
  page_title STRING,
  country STRING,
  score FLOAT);

INSERT INTO TABLE ellery.trending_alg1

SELECT 
    curr.project,
    curr.variant,
    curr.page_title,
    curr.country,
    curr.n / prev.n as score
FROM
(SELECT
    SUM(n) as n,
    project,
    variant,
    page_title,
    country
FROM ellery.geo_pageviews_v0_1
WHERE year=2015
AND month=1
AND day=2
GROUP by project, variant, page_title, country) prev

JOIN

(SELECT
    SUM(n) as n,
    project,
    variant,
    page_title,
    country
FROM ellery.geo_pageviews_v0_1
WHERE year=2015
AND month=2
AND day=2
GROUP by project, variant, page_title, country) curr

ON(
    prev.project = curr.project
    AND prev.variant = curr.variant
    AND  prev.page_title = curr.page_title
    AND prev.country = curr.country)
;

SELECT page_title, score from ellery.trending_alg1
WHERE variant = 'en'
AND project = 'wikipedia'
AND country = 'US'
ORDER BY score desc
LIMIT 1000;


SELECT geo.page_title, CONCAT(geo.year, '-', geo.month, '-', geo.day, ' '), SUM(geo.n) as n
FROM ellery.geo_pageviews_v0_1 geo
JOIN
(SELECT page_title, score from ellery.trending_alg1
WHERE variant = 'en'
AND project = 'wikipedia'
AND country = 'US'
ORDER BY score DESC
LIMIT 30) trend
WHERE geo.year = 2015
AND geo.variant = 'en'
AND geo.project = 'wikipedia'
AND geo.country = 'US'
AND geo.page_title = trend.page_title
GROUP BY geo.page_title, geo.year, geo.month, geo.day;


