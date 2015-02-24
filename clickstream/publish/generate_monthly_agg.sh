hadoop fs -copyToLocal /user/hive/warehouse/ellery.db/mc_jan mc_jan
cat mc_jan/* > 2015_01_clickstream_funny_null.tsv
sed 's/\\N//g' 2015_01_clickstream_funny_null.tsv > 2015_01_clickstream.tsv
gzip 2015_01_clickstream.tsv
scp stat1002.eqiad.wmnet:2015_01_clickstream.tsv.gz .
scp 2015_01_clickstream.tsv.gz stat1003.eqiad.wmnet:/srv/public-datasets/enwiki/clickstream/2015_01_clickstream.tsv.gz
