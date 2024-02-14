package com.gssystems.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        
        if (args == null || args.length != 2) {
            System.out.println(
                    "Need to pass 2 parameters - in and out directories!");
            System.exit(-1);
        }

        String infile = args[0];

        SparkSession spark = SparkSession.builder().appName("ReadJSON").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // these options are needed as the fields are quoted.
        Dataset<Row> tempsDF = spark.read().json(infile);
        tempsDF.printSchema();
        System.out.println("infile row count..." + tempsDF.count());

        System.out.println("Writing the convered csv to the directory " + args[1]);
        tempsDF.repartition(1).write().mode(SaveMode.Overwrite).option("header", "true").csv(args[1]);
        spark.close();
    }
}
