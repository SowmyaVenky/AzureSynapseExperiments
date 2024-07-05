package com.gssystems.dataset.compare;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ORCFileCreator {
    public static final boolean WRITE_FILE_OUTPUTS = true;
    public static void main(String[] args) {
        
        if (args == null || args.length != 2) {
            System.out.println(
                    "Need to pass 2 parameters - inputDS,  ORC Directory for this to work!");
            System.exit(-1);
        }

        String inputDS = args[0];
        String outputDS = args[1];

        SparkSession spark = SparkSession.builder().appName("Dataset creator").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> inDS = spark.read().parquet(inputDS);

        System.out.println("Reading input dataset to convert to ORC..");
        inDS.printSchema();
        System.out.println("Input Datset count: " + inDS.count());

        if (WRITE_FILE_OUTPUTS) {
            inDS.repartition(1).write().mode(SaveMode.Overwrite).orc(outputDS);
        }
        spark.close();
    }
}
