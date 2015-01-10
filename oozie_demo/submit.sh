#!/bin/bash

HDFS_USER=$(grep '^user[ ]*=' job.properties | cut -f 2 -d = | tr -d ' ')

hdfs dfs -mkdir -p "hdfs://analytics-hadoop/user/$HDFS_USER/demo-ellery" 2>&1 | grep -v SLF4J
hdfs dfs -put -f * "hdfs://analytics-hadoop/user/$HDFS_USER/demo-ellery" 2>&1 | grep -v SLF4J

# Letting hdfs settle for a bit
sleep 5

oozie job -config "job.properties" -run

cat <<EOF

                             ^
                             ^
                             ^
                             ^
The part after the 'job:' in ^  is the Oozie job id you want to look at.

It should be something like 0005079-141210154539499-oozie-oozi-W


You can follow its status through Hue, or also by running

  watch -n 10 oozie job -verbose -info INSERT_OOZIE_JOB_ID_HERE
EOF
