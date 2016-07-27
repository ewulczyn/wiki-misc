import os
import re
from db_utils import exec_hive_stat2
import argparse
import json

"""
python wikidata_utils.py \
--day 20160321 \
--dowload_dump

spark-submit \
    --driver-memory 5g \
    --master yarn \
    --deploy-mode client \
    --num-executors 10 \
    --executor-memory 10g \
    --executor-cores 4 \
    --queue priority \
/home/ellery/wmf/util/wikidata_utils.py \
    --day 20160321 \
    --extract_wills \
    --create_table \
    --db a2v
"""



def get_wikidata_dump(day,  path = '/user/ellery/wikidata_dumps'):
    print('Starting Download of Wikidata Dump')
    dump_name = os.path.join(path, day + '_dump')
    os.system('hadoop fs -rm -r -f %s' % dump_name)
    url = 'http://dumps.wikimedia.org/wikidatawiki/entities/%s/wikidata-%s-all.json.gz' % (day, day)
    ret = os.system("wget -O - %s | gunzip | hadoop fs -put - %s" % (url, dump_name))
    assert ret == 0, 'Loading Wikidata Dump Failed'
    print('Completed Download of Wikidata Dump')


def extract_WILLs(sc, day, path = '/user/ellery/wikidata_dumps'):
    dumpfile = os.path.join(path, day + '_dump')
    dump = sc.textFile(dumpfile)
    flat = dump.flatMap(get_agg_sitelinks).map(site_links_to_str)
    agg = dump.map(get_agg_sitelinks).filter(lambda x: len(x) > 0).map(agg_site_links_to_str)

    flat_will_path = os.path.join(path, day + '_flat_will')
    agg_will_path = os.path.join(path, day + '_agg_will')

    os.system('hadoop fs -rm -r ' + flat_will_path)
    os.system('hadoop fs -rm -r ' + agg_will_path)

    flat.saveAsTextFile (flat_will_path, compressionCodecClass= "org.apache.hadoop.io.compress.GzipCodec")
    agg.saveAsTextFile (agg_will_path, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec" )


def site_links_to_str(row):
    return '\t'.join(row)


def agg_site_links_to_str(rows):
    s = [rows[0][0]]
    for row in rows:
        s.append(row[1] + '|' + row[2])
    return '\t'.join(s)
        

def get_agg_sitelinks(line):
    p = re.compile('.*wiki$')
    
    item = None

    try:
        item = json.loads(line.rstrip('\n,'))
    except:
        return []

    item_id = item['id']
    
    if not item_id.startswith('Q'):
        return []
    links = item['sitelinks']
    
    rows = []
    for k, d in links.items():
        wiki = d['site']
        if p.match(wiki): 
            title = d['title']
            lang = wiki[:-4]
            rows.append([item_id, lang , title.replace(' ', '_')])
    return rows

def make_flat_will_hive_table(day, db, path = '/user/ellery/wikidata_dumps', table = 'wikidata_will'):
    flat_will_path = os.path.join(path, day + '_flat_will')
    query = """
    CREATE DATABASE IF NOT EXISTS %(db)s;
    DROP TABLE If EXISTS %(db)s.%(table)s;
    CREATE EXTERNAL TABLE %(db)s.%(table)s (
        id STRING,
        lang STRING,
        title STRING
    ) ROW FORMAT
    DELIMITED FIELDS TERMINATED BY '\t'
    LINES TERMINATED BY '\n' 
    STORED AS TEXTFILE
    LOCATION '%(path)s';
    """
    params = {
        'db': db,
        'table': table,
        'path': flat_will_path
    }
    print(query % params)
    exec_hive_stat2(query % params)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--day', required = True)
    parser.add_argument('--download_dump', default = False, action = 'store_true')
    parser.add_argument('--extract_wills', default = False, action = 'store_true')
    parser.add_argument('--create_table', default = False, action = 'store_true')
    parser.add_argument('--db', required = False)
    args = parser.parse_args()

    if args.download_dump:
        get_wikidata_dump(args.day)

    if args.extract_wills:
        from pyspark import SparkConf, SparkContext
        conf = SparkConf()
        conf.set("spark.app.name", 'finding missing articles')
        sc = SparkContext(conf=conf)
        extract_WILLs(sc, args.day)
    if args.create_table:
        make_flat_will_hive_table(args.day, args.db)

    