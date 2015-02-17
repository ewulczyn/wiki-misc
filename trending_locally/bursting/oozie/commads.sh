ssh -v bast1001.wikimedia.org

rsync -rv ~/wmf/trending_locally/ stat1002.eqiad.wmnet:~/wmf/trending_locally


rsync -rv ~/wmf/trending_locally/geo_pageviews_daily/oozie/ stat1002.eqiad.wmnet:~/wmf/trending_locally/geo_pageviews_daily/oozie



hadoop fs -mkdir /user/ellery/geo_pageviews_daily
hadoop fs -mkdir /user/ellery/geo_pageviews_daily/oozie


hadoop fs -rm -r -f  /user/ellery/geo_pageviews_daily/oozie/*
hadoop fs -put ~/wmf/trending_locally/geo_pageviews_daily/oozie/* /user/ellery/geo_pageviews_daily/oozie/
oozie job -oozie http://analytics1027.eqiad.wmnet:11000/oozie -run -config ~/wmf/trending_locally/geo_pageviews_daily/oozie/coordinator.properties
