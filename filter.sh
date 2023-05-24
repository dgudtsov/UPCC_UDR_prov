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

for INPUT in ${INPUT}/*.txt
do

	echo processing $INPUT

	NAME=`basename $INPUT .txt`

	OUTPUT=${DIR}/${NAME}_csv.txt
	SLAVE=${DIR}/Slave_${NAME}_csv.txt
	MASTER=${DIR}/Master_${NAME}_csv.txt

	time cat $INPUT | sed ':a;N;$!ba;s/;\n/;/g;s/<SUBEND//g'  |grep SID= >$OUTPUT
    
	echo generating $SLAVE
	cat $OUTPUT | grep STATION=2 >$SLAVE &
	PID=$!
		
	echo generating $MASTER
	time cat $OUTPUT | grep STATION=1 >$MASTER

	wait $PID

	rm $OUTPUT
	
	echo done $INPUT
	ls -la $INPUT $MASTER $SLAVE

done



