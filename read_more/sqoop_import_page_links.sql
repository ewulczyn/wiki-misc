export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64


sqoop import                                                        \
  --connect jdbc:mysql://s1-analytics-slave.eqiad.wmnet/enwiki      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_XXXXXX)      \
  --delete-target-dir                                               \
  --username=research --password                          \
  --query '
SELECT
  a.pl_from AS pl_from,
  a.pl_namespace as pl_namespace,
  CAST(a.pl_title AS CHAR(255) CHARSET utf8) AS pl_title,
  a.pl_from_namespace as pl_from_namespace
FROM pagelinks a
WHERE $CONDITIONS
'                                                                   \
--split-by a.pl_from                                                \
--hive-import                                                       \
--hive-database ellery                                                \
--create-hive-table                                                 \
--hive-table en_pagelinks
