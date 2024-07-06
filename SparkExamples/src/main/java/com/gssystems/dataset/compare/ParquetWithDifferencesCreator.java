package com.gssystems.dataset.compare;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ParquetWithDifferencesCreator {
    public static final boolean WRITE_FILE_OUTPUTS = true;

    public static void main(String[] args) {

        if (args == null || args.length != 2) {
            System.out.println(
                    "Need to pass 2 parameters - inputDS,  Parquet Directory for this to work!");
            System.exit(-1);
        }

        String inputDS = args[0];
        String outputDS = args[1];

        SparkSession spark = SparkSession.builder().appName("Dataset creator").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> inDS = spark.read().parquet(inputDS);

        System.out.println("Reading input dataset to convert to Parquet..with mismatches");
        inDS.printSchema();
        System.out.println("Input Datset count: " + inDS.count());

        // Creating a new col with the log of the actual col to create mismatches.
        Dataset<Row> inDSWithMismatches = inDS.withColumn("temperature_2m_new",
                org.apache.spark.sql.functions.log(inDS.col("temperature_2m")));

        Dataset<Row> inDSWithMismatches1 = inDSWithMismatches.drop("temperature_2m")
                .withColumnRenamed("temperature_2m_new", "temperature_2m");

        if (WRITE_FILE_OUTPUTS) {
            inDSWithMismatches1.repartition(1).write().mode(SaveMode.Overwrite).parquet(outputDS);
        }
        spark.close();
    }
}
