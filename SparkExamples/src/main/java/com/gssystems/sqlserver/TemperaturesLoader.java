package com.gssystems.sqlserver;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.delta.tables.DeltaTable;

public class TemperaturesLoader {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 1) {
            System.out.println(
                    "Need to pass 1 parameters - path to the delta table!");
            System.exit(-1);
        }

        String connectionURL = "jdbc:sqlserver://localhost:1433;DatabaseName=temperaturesdb;Encrypt=True;TrustServerCertificate=True";

        String temperaturesDeltaDir = args[0];

        SparkSession spark = SparkSession.builder().appName("Temperatures Delta").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("Reading the delta table from disk...");
        DeltaTable table1 = DeltaTable.forPath(spark, temperaturesDeltaDir);
        Dataset<Row> theDataSet = table1.df();
        theDataSet.printSchema();
        System.out.println("Read rows " + theDataSet.count());

        Properties props = new Properties();
        props.put("user", "sa");
        props.put("password", "Ganesh20022002");

        System.out.println("Writing data to sql server...");
        theDataSet.write().mode("overwrite")
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")           
            .format("jdbc")
            .option("url", connectionURL)
            .option("dbtable", "temperatures")
            .option("user", "sa")
            .option("password", "Ganesh20022002")
            .save();

        spark.close();
    }

}
