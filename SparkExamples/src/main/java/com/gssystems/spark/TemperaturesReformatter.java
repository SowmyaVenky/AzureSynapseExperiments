package com.gssystems.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TemperaturesReformatter {
	private static final boolean WRITE_FILE_OUTPUTS = true;
	public static void main(String[] args) {
		
		if (args == null || args.length != 2) {
			System.out.println("Need to pass 1 parameters - directory of the downloaded temperature files for this to work!");
			System.exit(-1);
		}

		String temperaturesDir = args[0];

		SparkSession spark = SparkSession.builder().appName("Temperatures").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// these options are needed as the fields are quoted.
		Dataset<?> tempsDF = spark.read().json(temperaturesDir);
		tempsDF.printSchema();

		Dataset<?> timeValuesDF = tempsDF.select(
			org.apache.spark.sql.functions.explode(tempsDF.col("hourly.time").alias("time"))
		);

		timeValuesDF.printSchema();		
		timeValuesDF.show();

		Dataset<?> tempValuesDF = tempsDF.select(
			org.apache.spark.sql.functions.explode(tempsDF.col("hourly.temperature_2m").alias("reading"))
		);
		
		tempValuesDF.printSchema();		
		tempValuesDF.show();

		if (WRITE_FILE_OUTPUTS) {
			System.out.println("Writing reformatted temperatures file...");
		}

		spark.close();
	}

}
