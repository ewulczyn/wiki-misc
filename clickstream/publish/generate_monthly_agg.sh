year=2015
month=02
clickstream_version=clickstream_v0_6


AGG_TABLE=agg_${clickstream_version}_${year}_${month}
FUNNY_NULL_TSV=${year}_${month}_clickstream_funny_null.tsv
FINAL_TSV=${year}_${month}_clickstream.tsv


hadoop fs -copyToLocal /user/hive/warehouse/ellery.db/${AGG_TABLE} ${AGG_TABLE}
cat header.txt ${AGG_TABLE}/* > ${FUNNY_NULL_TSV}
sed 's/\\N//g' ${FUNNY_NULL_TSV} > ${FINAL_TSV}
gzip ${FINAL_TSV}


scp stat1002.eqiad.wmnet:${FINAL_TSV}.gz /Users/ellerywulczyn/wmf/clickstream/data/${FINAL_TSV}.gz
scp ${FINAL_TSV}.gz stat1003.eqiad.wmnet:/srv/public-datasets/enwiki/clickstream/${FINAL_TSV}.gz


rm -r ${AGG_TABLE}
rm ${FUNNY_NULL_TSV}
rm ${FINAL_TSV}
rm ${FINAL_TSV}.gz