#!/bin/bash

BASEDIR=$(dirname $0)

spark-submit --class gr.ntua.bigdata.SparkMapReduceUsingRDDs \
	--num-executors 2 \
	--driver-memory 3g \
	--executor-memory 3g \
	--executor-cores 2 \
	${BASEDIR}/../target/sparkProcessing-1.0-SNAPSHOT.jar \
		-appName big-data-spark-rdd-api \
		-master spark://master:7077  \
		-inputTripDataPath hdfs://master:9000/user/input_data/yellow_tripdata_1m.csv \
		-inputTripVendorsPath hdfs://master:9000/user/input_data/yellow_tripvendors_1m.csv \
		-resultsPath hdfs://master:9000/user/results/ \
	2>&1 | tee -a /home/user/bigdata_project/logs/big-data-spark-rdd-api.`date '+%Y%m%d'`.log

exit_status=$?
#[[ ${exit_status} -ne 0 ]] && echo "[`date '+%Y-%m-%d %T'`] - ERROR: Spark job failed!" || echo "[`date '+%Y-%m-%d %T'`] - INFO: Spark job finished successfully!"
