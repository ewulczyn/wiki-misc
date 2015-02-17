ssh -v bast1001.wikimedia.org


rsync -r ~/wmf/trending_locally/geo_pageviews/oozie/ stat1002.eqiad.wmnet:~/wmf/trending_locally/geo_pageviews/oozie

rsync -r ~/wmf/ stat1002.eqiad.wmnet:~/wmf


hadoop fs -mkdir /user/ellery/geo_pageviews
hadoop fs -mkdir /user/ellery/geo_pageviews/oozie
hadoop fs -rm -r -f  /user/ellery/geo_pageviews/oozie/*

hadoop fs -put ~/wmf/trending_locally/geo_pageviews/oozie/* /user/ellery/geo_pageviews/oozie/
oozie job -oozie http://analytics1027.eqiad.wmnet:11000/oozie -run -config ~/wmf/trending_locally/geo_pageviews/oozie/coordinator.properties
