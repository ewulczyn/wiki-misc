
import os
import sys
import argparse
import json

# main namespace articles (no redirects)
page_sqoop_query = """
sqoop import                                                        \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s    \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_XXXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                       \
  --split-by a.page_id                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                       \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                   \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  a.page_id AS page_id,
  CAST(a.page_title AS CHAR(255) CHARSET utf8) AS page_title
FROM page a
WHERE $CONDITIONS AND page_namespace = 0 AND page_is_redirect = 0
'  
"""                                        

# main namespace redirects
redirect_sqoop_query = """
sqoop import                                                        \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_1XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                       \
  --split-by b.rd_from                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                        \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                          \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  b.rd_from AS rd_from,
  CAST(b.rd_title AS CHAR(255) CHARSET utf8) AS rd_title
FROM redirect b
WHERE $CONDITIONS AND rd_namespace = 0
'   
"""              

langlinks_sqoop_query = """
sqoop import                                                      \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_2XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                       \
  --split-by a.ll_from                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                        \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                         \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  a.ll_from AS ll_from,
  CAST(a.ll_title AS CHAR(255) CHARSET utf8) AS ll_title,
  CAST(a.ll_lang AS CHAR(20) CHARSET utf8) AS ll_lang
FROM langlinks a
WHERE $CONDITIONS
'
"""


revision_sqoop_query = """
sqoop import                                                      \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_2XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                       \
  --split-by rev_parent_id                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                        \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                          \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  rev_page,
  rev_user,
  CAST(rev_user_text AS CHAR(255) CHARSET utf8) AS rev_user_text,
  rev_minor_edit,
  rev_deleted,
  rev_len,
  rev_parent_id
FROM revision
WHERE $CONDITIONS
'
"""

pagelinks_sqoop_query = """
sqoop import                                                  \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_2XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                                            \
  --split-by a.pl_from                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                            \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                          \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  a.pl_from AS pl_from,
  CAST(a.pl_title AS CHAR(255) CHARSET utf8) AS pl_title
FROM pagelinks a
WHERE pl_namespace = 0
AND pl_from_namespace = 0
AND $CONDITIONS
'
"""

page_props_sqoop_query = """
sqoop import                                                  \
  --connect jdbc:mysql://analytics-store.eqiad.wmnet/%(mysql_db)s      \
  --verbose                                                         \
  --target-dir /tmp/$(mktemp -u -p '' -t ${USER}_sqoop_2XXXXX)      \
  --delete-target-dir                                               \
  --username research                                               \
  --password-file /user/ellery/sqoop.password                       \
  --split-by a.pp_page                                              \
  --hive-import                                                     \
  --hive-database %(hive_db)s                                            \
  --create-hive-table                                               \
  --hive-table %(result_table)s                                          \
  --hive-delims-replacement ' '                                  \
  --query '
SELECT
  a.pp_page AS pp_page,
  CAST(a.pp_propname AS CHAR(60) CHARSET utf8) AS pp_propname
FROM page_props a
WHERE $CONDITIONS
'
"""

page_props_join_query = """
CREATE TABLE  %(hive_db)s.%(result_table)s 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
AS SELECT 
regexp_replace(b.page_title, ' ', '_') as page_title,
pp_propname as propname
FROM %(hive_db)s.%(raw_table)s a JOIN %(hive_db)s.%(raw_page_table)s b ON ( a.pp_page = b.page_id)
"""


pagelinks_join_query = """
CREATE TABLE  %(hive_db)s.%(result_table)s 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
AS SELECT 
regexp_replace(b.page_title, ' ', '_') as pl_from,
regexp_replace(a.pl_title, ' ', '_') as pl_to
FROM %(hive_db)s.%(raw_table)s a JOIN %(hive_db)s.%(raw_page_table)s b ON ( a.pl_from = b.page_id)
"""


redirect_join_query = """
CREATE TABLE  %(hive_db)s.%(result_table)s 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
AS SELECT 
regexp_replace(b.page_title, ' ', '_') as rd_from,
regexp_replace(a.rd_title, ' ', '_') as rd_to
FROM %(hive_db)s.%(raw_table)s a JOIN %(hive_db)s.%(raw_page_table)s b ON ( a.rd_from = b.page_id)
"""

langlinks_join_query = """
CREATE TABLE  %(hive_db)s.%(result_table)s 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
AS SELECT 
regexp_replace(b.page_title, ' ', '_' ) as ll_from,
regexp_replace(a.ll_title, ' ', '_')  as ll_to,
a.ll_lang as ll_lang
FROM %(hive_db)s.%(raw_table)s a JOIN %(hive_db)s.%(raw_page_table)s b ON ( a.ll_from = b.page_id)
"""

queries = {
  'page' : {'sqoop': page_sqoop_query},
  'redirect': {'sqoop': redirect_sqoop_query,  'join': redirect_join_query},
  'langlinks': {'sqoop': langlinks_sqoop_query, 'join': langlinks_join_query},
  'pagelinks': {'sqoop': pagelinks_sqoop_query, 'join': pagelinks_join_query},
  'page_props': {'sqoop': page_props_sqoop_query, 'join': page_props_join_query},
  'revision': {'sqoop': revision_sqoop_query, 'join': 'NOT IMPLEMENTED'},
}


def exec_hive(statement):
  cmd =  """hive -e " """ + statement + """ " """
  print (cmd)
  ret =  os.system( cmd )
  assert ret == 0
  return ret

def exec_sqoop(statement):
  ret =  os.system(statement)
  assert ret == 0
  return ret


def sqoop_prod_dbs(db, langs, tables):
  ret = 0

  # make this is a  priority job
  ret += os.system("export HIVE_OPTS='-hiveconf mapreduce.job.queuename=priority'")

  # create the db if it does not exist
  create_db = 'CREATE DATABASE IF NOT EXISTS %(hive_db)s;'
  params = {'hive_db':db}
  ret += exec_hive(create_db % params)

  # delete table before creating them
  delete_query = "DROP TABLE IF EXISTS %(hive_db)s.%(result_table)s; "


  # sqoop requested tables into db
  ret += os.system("export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64")

  for lang in langs:
    for table in tables:

      params = {'hive_db': db,
               'mysql_db' : lang + 'wiki', 
               'result_table': lang + '_' + table + '_raw',
               }

      ret += exec_hive(delete_query % params)
      ret += exec_sqoop(queries[table]['sqoop'] % params)



    
  # join sqooped tables with page table to get clean final table

  for lang in langs:
      for table in tables:

        params = {'hive_db': db,
               'raw_page_table': lang + '_' + 'page' + '_raw',
               'raw_table': lang + '_' + table + '_raw',
               'result_table': lang + '_'  + table,
               }


        if table == 'page':
          continue

        ret += exec_hive(delete_query % params)
        ret += exec_hive(queries[table]['join'] % params)


       

  assert ret ==0

if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument('--db', required = True, help='path to recommendation file' )
  parser.add_argument('--langs', required = True,  help='comma seperated list of languages' )
  parser.add_argument('--tables',  required = True )
  args = parser.parse_args()
  langs = args.langs.split(',')
  tables = args.tables.split(',') 
  db = args.db
  sqoop_prod_dbs(db, langs, tables)




