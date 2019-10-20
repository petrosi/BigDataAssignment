package gr.ntua.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

public class SparkJoin {

	private static final String TRIP_ID = "id";

	public static final String TRIP_DATA_SCHEMA_FIELDS = "id startDate endDate startLongtitude startLatitude endLongtitude endLatitude cost";
	
	public static final String TRIP_VENDORS_SCHEMA_FIELDS = "id vendor";
	
	private static final Logger LOG = LoggerFactory.getLogger(SparkJoin.class);
	
	
	public static boolean startRunner(String name, SparkSettings settings, String[] args) {
	      JCommander jc = null;
	      try {
	         jc = new JCommander(settings);
	         jc.setProgramName(name);
	         jc.parse(args);
	      } catch (Exception e) {
	         LOG.error("", e);
	    	 System.out.println(e);
	         jc.usage();
	         return false;
	      }

	      return true;
	}
	
	public static void main(String[] args) {

		long startTime, endTime;
		
		// Parse command line arguments
		SparkSettings settings = new SparkSettings();
		
    	if (!startRunner(SparkJoin.class.getSimpleName(), settings, args)) {
    		LOG.error("Invalid settings. Exit.");
            return;
         }
   	
    	
    	LOG.info("Settings: appName: {}, master: {}, localTripDataPath: {}, localTripVendorsPath: {}, outputTripDataPath: {},"
    			+ " outputTripVendorsPath: {}", settings.appName, settings.master, settings.inputTripDataPath, 
    			settings.inputTripVendorsPath, settings.outputTripDataPath, settings.outputTripVendorsPath);
    	
		SparkSession spark = SparkSession
			.builder()
			.master(settings.master)
			.appName(settings.appName)
			.getOrCreate();
    	
		// Loading parquet files in datasets
		startTime = System.currentTimeMillis();
			
		Dataset<Row> tripData = spark
			.read()
			.parquet(settings.inputTripDataPath);
    	
		Dataset<Row> tripVendors = spark
			.read()
			.parquet(settings.inputTripVendorsPath)
			.limit(100);
			
		endTime = System.currentTimeMillis();

    	LOG.info("Reading data in Parquet format done!");
    	LOG.info("Seconds elapsed for transforming into Parquet format: {} sec", (endTime -  startTime)/1000);
    			
		Dataset<Row> tripDataJoinOp = tripData
			.join(tripVendors, tripVendors.col(TRIP_ID)
			.equalTo(tripData.col(TRIP_ID)));
		
		tripDataJoinOp.explain(true);
    	
		startTime = System.currentTimeMillis();
		
		tripDataJoinOp.show(false);
    	
		endTime = System.currentTimeMillis();
		
    	LOG.info("Seconds elapsed for default join operation: {} sec", (endTime -  startTime)/1000);    	
    	
		spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1");
		
		tripDataJoinOp.explain(true);
    	
		startTime = System.currentTimeMillis();
		
		tripDataJoinOp.show(false);
    	    	
		endTime = System.currentTimeMillis();
		
    	LOG.info("Seconds elapsed for changed join operation: {} sec", (endTime -  startTime)/1000);
		
	}

}
