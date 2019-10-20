package gr.ntua.bigdata;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import com.beust.jcommander.JCommander;

public class SparkMapReduceUsingRDDs {

	private static final Logger LOG = LoggerFactory.getLogger(SparkMapReduceUsingRDDs.class);

	private static boolean startRunner(String name, SparkSettings settings, String[] args) {
		JCommander jc = null;
		try {
			jc = new JCommander(settings);
			jc.setProgramName(name);
			jc.parse(args);
		} catch (Exception e) {
			LOG.error("JCommander failed", e);
			System.out.println(e);
			jc.usage();
			return false;
		}

		return true;
	}

	public static void main(String[] args) throws ParseException {
		long startTime, endTime;

		// Parse command line arguments
		SparkSettings settings = new SparkSettings();

		if (!startRunner(SparkMapReduceUsingRDDs.class.getSimpleName(), settings, args)) {
			LOG.error("Invalid settings. Exit.");
			return;
		}

		LOG.info("Settings: appName: {}, master: {}, inputTripDataPath: {}, inputTripVendorsPath: {}", settings.appName,
				settings.master, settings.inputTripDataPath, settings.inputTripVendorsPath);

		SparkConf conf = new SparkConf()
				.setAppName(settings.appName)
				.setMaster(settings.master)
				.set("spark.hadoop.validateOutputSpecs", "false");

		JavaSparkContext sc = new JavaSparkContext(conf);

		startTime = System.currentTimeMillis();

		// Load CSV files in RDDs
		LOG.info("Loading yellow_tripdata_1m.csv in RDDs.");
		JavaRDD<String> tripData = sc.textFile(settings.inputTripDataPath);
		LOG.info("Loading of yellow_tripdata_1m.csv done!");

		LOG.info("Loading yellow_tripvendors_1m.csv in RDDs.");
		JavaRDD<String> tripVendors = sc.textFile(settings.inputTripVendorsPath);
		LOG.info("Loading of yellow_tripvendors_1m.csv done!");

		endTime = System.currentTimeMillis();

		double csvLoadingTime = (endTime - startTime) / 1000.d;

		/*
		 * QUERY 1 : Compute the average trip duration per start hour 
		 */

		startTime = System.currentTimeMillis();

		// Select average trip duration in minutes grouped by hour of day
		LOG.info("Computing average trip duration per pick-up hour..");

		// Map start hour to trip duration for every row and add value 1 for counter
		JavaPairRDD<String, Double> tripDataSumCount = tripData
				.mapToPair(row -> new Tuple2<>(extractStartHour(row), new Tuple2<>(calculateTripDuration(row), 1)))
				.reduceByKey((tuple1, tuple2) -> new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
				.mapToPair(tuple -> new Tuple2<String, Double>(tuple._1, tuple._2._1 / tuple._2._2))
				.sortByKey();

		endTime = System.currentTimeMillis();

		double q1ComputationTime = (endTime - startTime) / 1000;

		if (settings.debug){
			LOG.info("--------------------------- QUERY 1 ---------------------------");
			System.out.println(tripDataSumCount.collect());
			LOG.info("---------------------------------------------------------------");
		}

		startTime = System.currentTimeMillis();

		tripDataSumCount.coalesce(1).saveAsTextFile(settings.resultsPath + settings.q1ResultsFileName);

		endTime = System.currentTimeMillis();

		double q1WriteTime = (endTime - startTime) / 1000;

		startTime = System.currentTimeMillis();

		// Select max trip cost for each vendor company
		LOG.info("Computing max trip cost per vendor company..");

		JavaPairRDD<String, Double> tripDataIdCost = tripData
				.mapToPair(row -> new Tuple2<String, Double>(extractTripId(row), parseCost(row)));

		JavaPairRDD<String, String> tripVendorsMapped = tripVendors
				.mapToPair(row -> new Tuple2<>(extractTripId(row), extractVendorId(row)));

		JavaPairRDD<String, Double> maxVendorIdCost = tripDataIdCost
				.join(tripVendorsMapped)
				.mapToPair(tuple -> new Tuple2<>(tuple._2._2, tuple._2._1))
				.reduceByKey((cost1, cost2) -> Double.max(cost1, cost2)).sortByKey();


		endTime = System.currentTimeMillis();

		double q2ComputationTime = (endTime - startTime) / 1000;

		if (settings.debug){
			LOG.info("--------------------------- QUERY 2 ---------------------------");
			System.out.println(maxVendorIdCost.collect());
			LOG.info("---------------------------------------------------------------");
		}

		startTime = System.currentTimeMillis();

		maxVendorIdCost.coalesce(1).saveAsTextFile(settings.resultsPath + settings.q2ResultsFileName);

		endTime = System.currentTimeMillis();
		
		double q2WriteTime = (endTime - startTime) / 1000;
	

		startTime = System.currentTimeMillis();

		LOG.info("Computing top 5 fastest trips..");

		List<Tuple2<Double, Long>> topFiveFastestTripsAndVendorIds = tripData
			.map(row -> toTripDataPOJO(row))
			.filter(x -> x.StartLongtitude <= 180.d && x.StartLongtitude >= -180.d)
			.filter(x -> x.EndLongtitude <= 180.d && x.EndLongtitude >= -180.d)
			.filter(x -> x.StartLatitude <= 90.d && x.StartLatitude >= -90.d)
			.filter(x -> x.EndLatitude <= 90.d && x.EndLatitude >= -90.d)
			.filter(x -> x.StartLongtitude != 0.d && x.StartLatitude != 0.d && x.EndLongtitude != 0.d && x.EndLatitude != 0.d)
			.filter(x -> x.StartLongtitude != x.EndLongtitude && x.StartLatitude != x.EndLatitude)
			.filter(x -> x.EndDate.compareTo(x.StartDate) > 0)
			.filter(x -> x.EndDate.compareTo(Timestamp.valueOf("2015-03-10 00:00:00")) > 0)
			.mapToPair(x -> new Tuple2<Long, Double>(x.TripId, speedInMetersPerSec(x)))
			.join(tripVendorsMapped.mapToPair(x -> new Tuple2<Long, Long>(Long.valueOf(x._1()), Long.valueOf(x._2()))))
			.mapToPair(x -> new Tuple2<Double, Long>(x._2()._1(), x._2()._2()))
			.sortByKey(false)
			.take(5)
			;
			
		endTime = System.currentTimeMillis();

		double q3ComputationTime = (endTime - startTime) / 1000;

		if (settings.debug){
			LOG.info("--------------------------- QUERY 3 ---------------------------");
			System.out.println(topFiveFastestTripsAndVendorIds);
			LOG.info("---------------------------------------------------------------");
		}

		startTime = System.currentTimeMillis();

		sc.parallelize(topFiveFastestTripsAndVendorIds).coalesce(1).saveAsTextFile(settings.resultsPath + settings.q3ResultsFileName);

		endTime = System.currentTimeMillis();
		
		double q3WriteTime = (endTime - startTime) / 1000;

		LOG.info("---------------------------------------------------------------");
		LOG.info("Time elapsed for loading CSV files to Datasets: {} sec", csvLoadingTime);
		LOG.info("---------------------------------------------------------------");

		LOG.info("--------------------------- QUERY 1 ---------------------------");
		LOG.info("Time elapsed for computing average trip duration per pick-up hour: {} sec", q1ComputationTime);
		LOG.info("Time elapsed for saving results to HDFS: {} sec", q1WriteTime);
		LOG.info("Time elapsed total: {} sec", q1ComputationTime + q1WriteTime);
		LOG.info("---------------------------------------------------------------");
		
		LOG.info("--------------------------- QUERY 2 ---------------------------");
		LOG.info("Time elapsed for computing max trip cost per company: {} sec", q2ComputationTime);
		LOG.info("Time elapsed for saving results to HDFS: {} sec", q2WriteTime);
		LOG.info("Time elapsed total: {} sec", q2ComputationTime + q2WriteTime);
		LOG.info("---------------------------------------------------------------");

		LOG.info("--------------------------- QUERY 3 ---------------------------");
		LOG.info("Time elapsed for computing top 5 fastest trips: {} sec", q3ComputationTime);
		LOG.info("Time elapsed for saving results to HDFS: {} sec", q3WriteTime);
		LOG.info("Time elapsed total: {} sec", q3ComputationTime + q3WriteTime);
		LOG.info("---------------------------------------------------------------");
    	
    	sc.close();
    }

	private static String extractStartHour(String row){
		String startDate = row.split(",")[1];
		String startHour = startDate.subSequence(11, 13).toString();
		return startHour;
	}

	private static Double calculateTripDuration(String row) throws ParseException {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String startDate = row.split(",")[1];
		String endDate = row.split(",")[2];
		Double duration = ((dateFormat.parse(endDate).getTime() - dateFormat.parse(startDate).getTime())/1000.0)/1000.0;
		return duration;
	}

	private static String extractTripId(String row) {
		return row.split(",")[0].toString();
	}
	
	private static Double parseCost(String row) {
		return Double.valueOf(row.split(",")[7]);
	}
	
	private static String extractVendorId(String row) {
		return row.split(",")[1];
	}

	private static final TripDataPOJO toTripDataPOJO(String row) {

		String[] tokens = row.split(",");

		Long id = Long.valueOf(tokens[0]);
		Timestamp startDate = Timestamp.valueOf(tokens[1]);
		Timestamp endDate = Timestamp.valueOf(tokens[2]);
		Double startLongtitude = Double.valueOf(tokens[3]);
		Double startLatitude = Double.valueOf(tokens[4]);
		Double endLongtitude = Double.valueOf(tokens[5]);
		Double endLatitude = Double.valueOf(tokens[6]);
		Double cost = Double.valueOf(tokens[7]);

		return new TripDataPOJO(id, startDate, endDate, startLongtitude, startLatitude, endLongtitude, endLatitude, cost);
	}

	private static Double haversineFunction(Double angle) {
		return Math.pow(Math.sin(angle / 2.0), 2);
	}

	private static Double haversineDistance(TripDataPOJO tripData) {
		Double phi1 = Math.toRadians(tripData.StartLatitude);
		Double phi2 = Math.toRadians(tripData.EndLatitude);
		Double deltaPhi = phi2 - phi1;
		Double l1 = Math.toRadians(tripData.StartLongtitude);
		Double l2 = Math.toRadians(tripData.EndLongtitude);
		Double deltaL = l2 - l1;
		Double haversineDistance = haversineFunction(deltaPhi) + Math.cos(phi1)* Math.cos(phi2) * haversineFunction(deltaL);
		return haversineDistance;
	}

	private static final Double speedInMetersPerSec(TripDataPOJO tripData){
		Double a = haversineDistance(tripData);
		Double c = 2.d * Math.atan2(Math.sqrt(a), Math.sqrt(1.d - a));
		Double R = 6371.d;
		Double distance = R * c;

		Double duration = (tripData.EndDate.getTime() - tripData.StartDate.getTime()) / 1000.d;

		Double speed = distance / duration;

		return speed;
	}
}
