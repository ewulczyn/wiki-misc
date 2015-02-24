ssh -v bast1001.wikimedia.org
rsync -rv ~/wmf/trending_locally/ stat1002.eqiad.wmnet:~/wmf/trending_locally
rsync -rv ~/wmf/trending_locally/trending/oozie/ stat1002.eqiad.wmnet:~/wmf/trending_locally/trending/oozie



hadoop fs -mkdir /user/ellery/trending
hadoop fs -mkdir /user/ellery/trending/oozie


hadoop fs -rm -r -f  /user/ellery/trending/oozie/*
hadoop fs -put ~/wmf/trending_locally/trending/oozie/* /user/ellery/trending/oozie/
oozie job -oozie http://analytics1027.eqiad.wmnet:11000/oozie -run -config ~/wmf/trending_locally/trending/oozie/coordinator.properties
