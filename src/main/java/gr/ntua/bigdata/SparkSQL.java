package gr.ntua.bigdata;

import java.util.Arrays;
import java.util.List;
import java.text.ParseException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Column;

import com.beust.jcommander.JCommander;

public class SparkSQL {

	private static final Logger LOG = LoggerFactory.getLogger(SparkSQL.class);

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

	private static StructType createTripDataSchema() {

		List<StructField> fields = Arrays.asList(
				DataTypes.createStructField("id", DataTypes.LongType, false),
				DataTypes.createStructField("startDate", DataTypes.TimestampType, false),
				DataTypes.createStructField("endDate", DataTypes.TimestampType, false),
				DataTypes.createStructField("startLongtitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("startLatitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("endLongtitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("endLatitude", DataTypes.DoubleType, false),
				DataTypes.createStructField("cost", DataTypes.DoubleType, false));
				
		return DataTypes.createStructType(fields);
	}

	private static StructType createTripVendorsSchema() {
		List<StructField> fields = Arrays.asList(
			DataTypes.createStructField("id", DataTypes.LongType, false),
			DataTypes.createStructField("vendor", DataTypes.IntegerType, false)
		);
		return DataTypes.createStructType(fields);
	}

	private static Column haversineFunction(Column angle) {
		return pow(sin(angle.divide(2.0)), 2);
	}

	private static Column haversineDistance(Column startLongtitude, Column endLongtitude, Column startLatitude,
			Column endLatitude) {
		Column phi1 = radians(startLatitude);
		Column phi2 = radians(endLatitude);
		Column deltaPhi = phi2.minus(phi1);
		Column l1 = radians(startLongtitude);
		Column l2 = radians(endLongtitude);
		Column deltaL = l2.minus(l1);
		Column haversineDistance = haversineFunction(deltaPhi)
									.plus(
										cos(phi1)
											.multiply(cos(phi2))
											.multiply(haversineFunction(deltaL)));
		return haversineDistance;
	}

	public static void main(String[] args) throws ParseException {

		long startTime, endTime;
		final StructType customTripDataSchema, customTripVendorsSchema;

		// Parse command line arguments
		SparkSettings settings = new SparkSettings();

		if (!startRunner(SparkSQL.class.getSimpleName(), settings, args)) {
			LOG.error("Invalid settings. Exit.");
			return;
		}

		LOG.info(
				"Settings: appName: {}, master: {}, localTripDataPath: {}, localTripVendorsPath: {}, outputTripDataPath: {},"
						+ " outputTripVendorsPath: {}",
				settings.appName, settings.master, settings.inputTripDataPath, settings.inputTripVendorsPath,
				settings.outputTripDataPath, settings.outputTripVendorsPath);

		// Create schemas for the two CSV files
		customTripDataSchema = createTripDataSchema();
		customTripVendorsSchema = createTripVendorsSchema();

		SparkSession spark = SparkSession
								.builder()
								.master(settings.master)
								.appName(settings.appName)
								.getOrCreate();

		// Load CSV files in Datasets

		startTime = System.currentTimeMillis();

		LOG.info("Loading yellow_tripdata_1m.csv in a Dataset.");

		Dataset<Row> tripData = spark
				.read()
				.format("csv")
				.option("sep", ",")
				.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
				.schema(customTripDataSchema)
				.load(settings.inputTripDataPath);

		LOG.info("Loading of yellow_tripdata_1m.csv done!");

		LOG.info("Loading yellow_tripvendors_1m.csv in a Dataset.");

		Dataset<Row> tripVendors = spark.
				read()
				.format("csv")
				.option("sep", ",")
				.schema(customTripVendorsSchema)
				.load(settings.inputTripVendorsPath);

		LOG.info("Loading of yellow_tripvendors_1m.csv done!");

		endTime = System.currentTimeMillis();

		double csvLoadingTime = (endTime - startTime) / 1000.d;

		String fileFormat = "csv";

		double parquetTransformationTime = 0.d;

		if (settings.useParquet) {
				
			fileFormat = "parquet";

			// Transforming into Parquet format

			LOG.info("Writing input CSV files to HDFS in Parquet format.");

			startTime = System.currentTimeMillis();

			tripData
				.write()
				.mode(SaveMode.Overwrite)
				.parquet(settings.outputTripDataPath);

			tripVendors
				.write()
				.mode(SaveMode.Overwrite)
				.parquet(settings.outputTripVendorsPath);

			endTime = System.currentTimeMillis();

			LOG.info("Writing of input CSV files to HDFS in Parquet format done!");

			LOG.info("---------------------------------------------------------------");
			LOG.info("Seconds elapsed for writing into Parquet format: {} sec", (endTime - startTime) / 1000);
			LOG.info("---------------------------------------------------------------");

			LOG.info("Reading data in Parquet format..");

			tripData = spark
						.read()
						.parquet(settings.outputTripDataPath);

			tripVendors = spark
							.read()
							.parquet(settings.outputTripVendorsPath);
				
			LOG.info("Reading data in Parquet format done!");

			endTime = System.currentTimeMillis();

			parquetTransformationTime = (endTime - startTime) / 1000.d;

			LOG.info("---------------------------------------------------------------");
			LOG.info("Seconds elapsed for reloading into Parquet format: {} sec", parquetTransformationTime);
			LOG.info("---------------------------------------------------------------");

		}

		// tripData.where(col("id").equalTo(283468670624L)).show(false);
		tripData.where(col("id").equalTo(249108525463L)).show(false);

		/*
		 * QUERY 1 : Compute the average trip duration per start hour
		 */

		LOG.info("Computing average trip duration per pick-up hour..");

		startTime = System.currentTimeMillis();

		Dataset<Row> averageTripDurationPerStartHour = tripData
				.filter(col("endDate").gt(col("startDate")))
				.select(col("startDate").substr(12, 2).alias("HourOfDay"), (unix_timestamp(col("endDate")).minus(unix_timestamp(col("startDate")))).divide(1000.d).alias("duration"))
				.groupBy(col("HourOfDay"))
				.agg(avg("duration").alias("AverageTripDuration"))
				.orderBy("HourOfDay");

		endTime = System.currentTimeMillis();

		double q1ComputationTime = (endTime - startTime) / 1000;

		if (settings.debug){
			averageTripDurationPerStartHour.show(false);
		}

		startTime = System.currentTimeMillis();

		averageTripDurationPerStartHour
			.write()
			.format(fileFormat)
			.mode(SaveMode.Overwrite)
			.save(settings.resultsPath + settings.q1ResultsFileName + "." + fileFormat);
		
		endTime = System.currentTimeMillis();

		double q1WriteTime = (endTime - startTime) / 1000;

		/*
		 * Query 2: Find max trip cost for each vendor
		 */

		LOG.info("Computing max trip cost per vendor company..");

		startTime = System.currentTimeMillis();

		Dataset<Row> maxTripCostPerVendor = tripData
				.join(tripVendors, tripVendors.col("id").equalTo(tripData.col("id")))
				.groupBy(tripVendors.col("vendor").alias("VendorId"))
				.agg(max(tripData.col("cost")).alias("MaxTripCost"))
				.orderBy("VendorId");

		endTime = System.currentTimeMillis();

		double q2ComputationTime = (endTime - startTime) / 1000;

		if (settings.debug){
			maxTripCostPerVendor.show(false);
		}

		startTime = System.currentTimeMillis();

		maxTripCostPerVendor
			.write()
			.format(fileFormat)
			.mode(SaveMode.Overwrite)
			.save(settings.resultsPath + settings.q2ResultsFileName + "." + fileFormat);
		
		endTime = System.currentTimeMillis();
		
		double q2WriteTime = (endTime - startTime) / 1000;
		
		/*
		 * Query 3: Find the 5 fastest trips and their vendors
		 */

		LOG.info("Computing top 5 fastest trips..");

		startTime = System.currentTimeMillis();

		Dataset<Row> parsedTripData = tripData
				// Clear the data
				.filter(col("startLongtitude").between(-180.d, 180.d))
				.filter(col("startLatitude").between(-90.d, 90.d))
				.filter(col("endLongtitude").between(-180.d, 180.d))
				.filter(col("endLatitude").between(-90.d, 90.d))
				.filter(not(col("startLongtitude").eqNullSafe(0.d)))
				.filter(not(col("startLatitude").eqNullSafe(0.d)))
				.filter(not(col("endLongtitude").eqNullSafe(0.d)))
				.filter(not(col("endLatitude").eqNullSafe(0.d)))
				.filter(col("startLongtitude").notEqual(col("endLongtitude")))
				.filter(col("startLatitude").notEqual(col("endLatitude")))
				.filter(col("endDate").gt(col("startDate")))
				// Take the trips after May 10
				.filter(col("startDate").geq(Timestamp.valueOf("2015-03-10 00:00:00")))
				.withColumn("durationInSec", unix_timestamp(col("endDate")).minus(unix_timestamp(col("startDate"))))
				.withColumn("a", haversineDistance(col("startLongtitude"), col("endLongtitude"), col("startLatitude"), col("endLatitude")))
				// Make sure 0 <= a <= 1 because of numerical stability issues
				.filter(col("a").leq(1.d))
				.filter(col("a").geq(0.d))
				.withColumn("c", atan2(sqrt(col("a")), sqrt(col("a").multiply(-1.d).plus(1.d))).multiply(2.d)) // c = 2 * atan2(sqrt(a), sqrt(1 - a))
				.withColumn("distanceInMeters", col("c").multiply(6371.d)) // d = c * R , where R = 6371m
				.withColumn("speedMetersPerSec", col("distanceInMeters").divide(col("durationInSec")))
				;

		// Join with vendor companies to find the vendor id
		Dataset<Row> firstFiveFastestTrips = parsedTripData
				.join(tripVendors, tripVendors.col("id").equalTo(parsedTripData.col("id")))
				.select(tripVendors.col("vendor").alias("VendorCompanyId"), parsedTripData.col("id").alias("TripId"),
						parsedTripData.col("speedMetersPerSec").alias("SpeedInMetersPerSec"),
						parsedTripData.col("distanceInMeters").alias("DistanceInMeters"),
						parsedTripData.col("durationInSec").alias("DurationInSec"))
				.orderBy(col("SpeedInMetersPerSec").desc())
				.limit(5)
				;

		endTime = System.currentTimeMillis();

		double q3ComputationTime = (endTime - startTime) / 1000;

		if (settings.debug){
			parsedTripData.explain(true);
			firstFiveFastestTrips.show(100, false);
		}

		startTime = System.currentTimeMillis();

		firstFiveFastestTrips
			.write()
			.format(fileFormat)
			.mode(SaveMode.Overwrite)
			.save(settings.resultsPath + settings.q3ResultsFileName + "." + fileFormat);
		
		endTime = System.currentTimeMillis();
		
		double q3WriteTime = (endTime - startTime) / 1000;
		
		LOG.info("---------------------------------------------------------------");
		LOG.info("Time elapsed for loading CSV files to Datasets: {} sec", csvLoadingTime);
		LOG.info("---------------------------------------------------------------");

		if (settings.useParquet){
			LOG.info("---------------------------------------------------------------");
			LOG.info("Time elapsed for transforimg CSV to Parquet format: {} sec", parquetTransformationTime);
			LOG.info("---------------------------------------------------------------");
		}


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

		spark.close();

	}

}
