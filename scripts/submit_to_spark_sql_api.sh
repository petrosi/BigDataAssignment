#!/bin/bash

BASEDIR=$(dirname $0)

spark-submit --class gr.ntua.bigdata.SparkSQL \
 	--num-executors 2 \
	--driver-memory 3g \
	--executor-memory 3g \
	--executor-cores 2 \
	${BASEDIR}/../target/sparkProcessing-1.0-SNAPSHOT.jar \
 		-appName big-data-spark-sql-api \
		-master spark://master:7077  \
		-inputTripDataPath hdfs://master:9000/user/input_data/yellow_tripdata_1m.csv \
		-inputTripVendorsPath hdfs://master:9000/user/input_data/yellow_tripvendors_1m.csv \
		-outputTripDataPath hdfs://master:9000/user/tripDataParquet \
		-outputTripVendorsPath hdfs://master:9000/user/tripVendorsParquet \
		-resultsPath hdfs://master:9000/user/results/ \
	2>&1 | tee -a /home/user/bigdata_project/logs/parquet_spark_processing.`date '+%Y%m%d'`.log

