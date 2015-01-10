ssh -v bast1001.wikimedia.org


rsync -r ~/wmf/ stat1002.eqiad.wmnet:~/wmf

hadoop fs -mkdir /user/ellery/
hadoop fs -mkdir /user/ellery/oozie_demo

hadoop fs -rm -r -f  /user/ellery/oozie_demo/*
hadoop fs -put ~/wmf/oozie_demo/* /user/ellery/oozie_demo/

oozie job -oozie http://analytics1027.eqiad.wmnet:11000/oozie -run -config ~/wmf/oozie_demo/coordinator.properties



CREATE TABLE IF NOT EXISTS ellery_demo_persistent(
    user_agent STRING,
    browser_family STRING
);