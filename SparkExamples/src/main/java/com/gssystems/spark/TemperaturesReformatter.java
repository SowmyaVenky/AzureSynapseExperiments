package com.gssystems.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class TemperaturesReformatter {
	private static final boolean WRITE_FILE_OUTPUTS = true;
	public static void main(String[] args) {
		
		if (args == null || args.length != 3) {
			System.out.println("Need to pass 2 parameters - directory of the downloaded temperature files for this to work!");
			System.exit(-1);
		}

		String temperaturesDir = args[0];

		SparkSession spark = SparkSession.builder().appName("Temperatures").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// these options are needed as the fields are quoted.
		Dataset<?> tempsDF = spark.read().json(temperaturesDir);
		tempsDF.printSchema();

		//Using the arrays_zip function to make both the arrays come together
		
		Dataset<?> timeAndTempDF = tempsDF.withColumn("tmp",
			org.apache.spark.sql.functions.arrays_zip(
				tempsDF.col("hourly.time"), 
				tempsDF.col("hourly.temperature_2m")
			)
		).drop("generationtime_ms", "hourly", "hourly_units", "timezone", "timezone_abbreviation", "utc_offset_seconds");

		//Exploding the  zipped array to create rows per time and temp.
		Dataset<?> timeAndTempDFexploded = timeAndTempDF.withColumn("tmp", 
			org.apache.spark.sql.functions.explode(timeAndTempDF.col("tmp")));

		Dataset<?> timeAndTempDFFinal = timeAndTempDFexploded.select(
			timeAndTempDFexploded.col("latitude"),
			timeAndTempDFexploded.col("longitude"),
			timeAndTempDFexploded.col("tmp.time"),
			timeAndTempDFexploded.col("tmp.temperature_2m")
		).drop("tmp");

		timeAndTempDFFinal.printSchema();		
		timeAndTempDFFinal.show();


		if (WRITE_FILE_OUTPUTS) {
			System.out.println("Writing reformatted temperatures file...");
			timeAndTempDFFinal.repartition(1).write().parquet(args[1]);
		}

		Dataset<?> locationMaster = timeAndTempDFexploded.select(
			timeAndTempDFexploded.col("elevation"),
			timeAndTempDFexploded.col("latitude"),
			timeAndTempDFexploded.col("longitude")
		).drop("tmp").distinct();

		if (WRITE_FILE_OUTPUTS) {
			System.out.println("Writing location master file...");
			locationMaster.repartition(1).write().parquet(args[2]);
		}
		
		spark.close();
	}

}
