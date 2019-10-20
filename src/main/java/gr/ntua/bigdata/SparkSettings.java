package gr.ntua.bigdata;

import com.beust.jcommander.Parameter;

public class SparkSettings {
	
	@Parameter(names = "-appName", required = false, description = "A name for your application, to display on the cluster web UI", echoInput = true)
	public String appName = "SparkSQL-parquet-app";
	
	@Parameter(names = "-master", required = false, description = "Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4])", echoInput = true)
	public String master = "localhost";
	
	@Parameter(names = "-inputTripDataPath", required = false, description = "Path to the tripdata file on the local file system", echoInput = true)
	public String inputTripDataPath = "/tmp/yellow_tripdata_1m.csv";
	
	@Parameter(names = "-inputTripVendorsPath", required = false, description = "Path to the tripvendors file on the local file system", echoInput = true)
	public String inputTripVendorsPath = "/tmp/yellow_tripvendors_1m.csv";
	
	@Parameter(names = "-outputTripDataPath", required = false, description = "Path to the output tripdata files on the HDFS", echoInput = true)
	public String outputTripDataPath = "hdfs://localhost:9000/tmp/tripDataCsv/";
	
	@Parameter(names = "-outputTripVendorsPath", required = false, description = "Path to the output tripvendors files on the HDFS", echoInput = true)
	public String outputTripVendorsPath = "hdfs://localhost:9000/tmp/tripVendorsCsv/";

	@Parameter(names = "-resultsPath", required = false, description = "Path to the results of the queries on HDFS", echoInput = true)
	public String resultsPath = "hdfs://localhost:9000/tmp/results/";
	
	@Parameter(names = "--parquet", required = false, description = "Set if a conversion from .csv to .parquet is needed", echoInput = true)
	public Boolean useParquet = false;

	@Parameter(names = "--debug", required = false, description = "Set if you want to debug the code", echoInput = true)
	public Boolean debug = false;

	public static final String q1ResultsFileName = "Q1_results";
	public static final String q2ResultsFileName = "Q2_results";
	public static final String q3ResultsFileName = "Q3_results";

}