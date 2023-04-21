#!/bin/bash

#INPUT=UPCC_3_20190905173300_1_huabiao1.txt.gz

#OUTPUT=records_all.txt.gz
#MASTER=records_master.txt.gz
#SLAVE=records_slave.txt.gz

#time zcat $INPUT | head -n 100000 | sed ':a;N;$!ba;s/;\n/;/g'  |grep SID= | gzip >$ALL

# input dir
INPUT=./input

# output dir
DIR=./csv

if [ ! -d $DIR ]
then
		mkdir $DIR
fi

for INPUT in ${INPUT}/*.txt.gz
do

	echo processing $INPUT

	NAME=`basename $INPUT .txt.gz`

	OUTPUT=${DIR}/${NAME}_csv.txt.gz
	SLAVE=${DIR}/Slave_${NAME}_csv.txt.gz
	MASTER=${DIR}/Master_${NAME}_csv.txt.gz

	time zcat $INPUT | sed ':a;N;$!ba;s/;\n/;/g'  |grep SID= | gzip >$OUTPUT
    
    echo generating $SLAVE
	zcat $OUTPUT | grep STATION=2 | gzip >$SLAVE &
	PID=$!
		
	echo generating $MASTER
	time zcat $OUTPUT | grep STATION=1 | gzip >$MASTER

    wait $PID

	rm $OUTPUT

done



