#!/bin/bash

BASEDIR=$(dirname $0)

spark-submit --class gr.ntua.bigdata.SparkJoin \
 	--num-executors 2 \
	--driver-memory 3g \
	--executor-memory 3g \
	--executor-cores 2 \
	${BASEDIR}/../target/sparkProcessing-1.0-SNAPSHOT.jar \
 		-appName big-data-spark-join \
		-master spark://master:7077  \
		-inputTripDataPath hdfs://master:9000/user/tripDataParquet \
		-inputTripVendorsPath hdfs://master:9000/user/tripVendorsParquet \
	2>&1 | tee -a /home/user/bigdata_project/logs/big-data-spark-join.`date '+%Y%m%d'`.log

