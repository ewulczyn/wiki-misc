ssh -v bast1001.wikimedia.org


rsync -rv ~/wmf/mc/oozie/ stat1002.eqiad.wmnet:~/wmf/mc/oozie

hadoop fs -mkdir /user/ellery/mc
hadoop fs -mkdir /user/ellery/mc/oozie




hadoop fs -rm -r -f  /user/ellery/mc/oozie/*
hadoop fs -put ~/wmf/mc/oozie/* /user/ellery/mc/oozie/
oozie job -oozie http://analytics1027.eqiad.wmnet:11000/oozie -run -config ~/wmf/mc/oozie/coordinator.properties
