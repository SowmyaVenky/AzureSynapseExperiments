package com.gssystems.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class GenerateHashedColumns {
    	private static final boolean WRITE_FILE_OUTPUTS = true;
		private static final String HASHED = "_H";
	public static void main(String[] args) {
		
		if (args == null || args.length != 3) {
			System.out.println("Usage : inputfile outputdir columnstohash");
			System.exit(-1);
		}

		String personDir = args[0];

		String columnsToHash = args[2];
		List<String> hashCols = new ArrayList<String>();
		if( columnsToHash != null ) {
			String[] cols = columnsToHash.split(",");
			for( String a: cols) {
				hashCols.add(a);
				System.out.println("Got hash col: " + a);
			} 
		}

		System.out.println("Going to hash " + hashCols.size() + " columns");
		
		SparkSession spark = SparkSession.builder().appName("Person").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		// these options are needed as the fields are quoted.
		Dataset<?> personDF = spark.read().json(personDir);
		personDF.printSchema();

		for( String a : hashCols) {
			personDF = personDF.withColumn(a + HASHED, org.apache.spark.sql.functions.sha2(personDF.col(a), 256));
		}

		personDF.printSchema();
		personDF.show();

		if (WRITE_FILE_OUTPUTS) {
			System.out.println("Writing hashed file...");
			personDF.repartition(1).write().parquet(args[1]);
		}

		spark.close();
	}
}
