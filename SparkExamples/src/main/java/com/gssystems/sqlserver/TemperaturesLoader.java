package com.gssystems.sqlserver;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.delta.tables.DeltaTable;

public class TemperaturesLoader {
    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            System.out.println(
                    "Need to pass 1 parameters - path to the delta table!");
            System.exit(-1);
        }

        String temperaturesDeltaDir = args[0];

        SparkSession spark = SparkSession.builder().appName("Temperatures Delta").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        DeltaTable table1 = DeltaTable.forPath(spark, temperaturesDeltaDir);
        Dataset<Row> theDataSet = table1.df();
        theDataSet.printSchema();

        Properties props = new Properties();
        props.put("user", "sa");
        props.put("password", "Ganesh20022002");

        theDataSet.write().jdbc("jdbc:sqlserver://localhost;databaseName=temperaturesdb;", "temperatues", props);
        spark.close();
    }

}
