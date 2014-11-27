import os

if os.path.isfile('.lock'):
	exit() 

else:
	os.system('touch .lock')
	os.system('/home/otto/kafkacat -C -b analytics1021.eqiad.wmnet,analytics1012.eqiad.wmnet,analytics1018.eqiad.wmnet,analytics1022.eqiad.wmnet -t webrequest_text -e -o stored -X topic.auto.commit.interval.ms=100 -X topic.offset.store.path=/home/ellery/record_impression/text_offsets | grep  /wiki/Special:RecordImpression | python /home/ellery/record_impression/import.py >/dev/null')
	os.system('/home/otto/kafkacat -C -b analytics1021.eqiad.wmnet,analytics1012.eqiad.wmnet,analytics1018.eqiad.wmnet,analytics1022.eqiad.wmnet -t webrequest_mobile -e -o stored -X topic.auto.commit.interval.ms=100 -X topic.offset.store.path=/home/ellery/record_impression/mobile_offsets | grep  /wiki/Special:RecordImpression | python /home/ellery/record_impression/import.py >/dev/null')
	os.system('rm .lock')
