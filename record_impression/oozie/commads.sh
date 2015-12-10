ssh -v bast1001.wikimedia.org


oozie job -info 

oozie jobs -jobtype coordinator -filter status=RUNNING
oozie job -kill 


rsync -r ~/wmf/record_impression/oozie/ stat1002.eqiad.wmnet:~/wmf/record_impression/oozie



hadoop fs -mkdir /user/ellery/record_impression
hadoop fs -mkdir /user/ellery/record_impression/oozie


hadoop fs -rm -r -f  /user/ellery/record_impression/oozie/*
hadoop fs -put ~/wmf/record_impression/oozie/* /user/ellery/record_impression/oozie/
oozie job -oozie http://analytics1027.eqiad.wmnet:11000/oozie -run -config ~/wmf/record_impression/oozie/coordinator.properties



set hivevar:version=test;
set hivevar:year=2015;
set hivevar:month=12;
set hivevar:day=1;
set hivevar:hour=1;