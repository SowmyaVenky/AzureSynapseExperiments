package com.gssystems.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class TemperaturesDeltaProcessing {
    private static final boolean WRITE_FILE_OUTPUTS = true;

    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            System.out.println(
                    "Need to pass 2 parameters - directory of the downloaded temperature files for this to work!");
            System.exit(-1);
        }

        String temperaturesParquetDir = args[0];

        SparkSession spark = SparkSession.builder().appName("Temperatures Delta").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // these options are needed as the fields are quoted.
        Dataset<?> tempsDF = spark.read().parquet(temperaturesParquetDir);
        tempsDF.printSchema();

        if (WRITE_FILE_OUTPUTS) {
            System.out.println("Writing delta temperatures file...");
            tempsDF.repartition(1).write().format("delta").save(args[1]);
        }

        spark.close();
    }
}
