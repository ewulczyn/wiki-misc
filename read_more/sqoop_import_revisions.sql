export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64


sqoop import                                                        \
  --connect jdbc:mysql://s1-analytics-slave.eqiad.wmnet/enwiki      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_XXXXXX)      \
  --delete-target-dir                                               \
  --username=research --password JoFjnA90Ajyp                         \
  --query '
SELECT
f.user_id,f.page_id,
CAST(f.page_title AS CHAR(255) CHARSET utf8) AS page_title
FROM
(SELECT r.rev_user as user_id, r.rev_page as page_id, p.page_title as page_title
FROM
(SELECT
page_id,page_title FROM page
WHERE page_namespace = 0) p
JOIN
(SELECT rev_user, rev_page
FROM revision
WHERE rev_user > 10000
AND rev_user < 12000
GROUP BY rev_user,rev_page ) r 
ON (r.rev_page=p.page_id)) f
WHERE $CONDITIONS
'                                                                   \
--split-by f.page_id                                                \
--hive-import                                                       \
--hive-database ellery                                                \
--create-hive-table                                                 \
--hive-table en_revisions






