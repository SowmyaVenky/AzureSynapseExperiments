package com.gssystems.delta;

import org.apache.spark.sql.SparkSession;

import io.delta.tables.DeltaTable;

public class TemperaturesDeltaReader {
    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            System.out.println(
                    "Need to pass 1 parameter - directory of the delta main table");
            System.exit(-1);
        }

        String temperaturesDeltaDir = args[0];

        SparkSession spark = SparkSession.builder().appName("Temperatures Delta").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        DeltaTable table1 = DeltaTable.forPath(spark, temperaturesDeltaDir);
        table1.df().filter("latitude = 61.199997 " + 
        " and longitude = -149.9 " +
        " and time in ('2019-01-01T00:00', " + 
        " '2019-01-01T01:00', '2019-01-01T02:00', " +
        " '2019-01-01T03:00', '2019-01-01T04:00', " +
        " '2019-01-01T05:00', '2019-01-01T06:00', " +
        " '2009-01-01T00:00', '2009-01-01T01:00', " + 
        " '2009-01-01T02:00' ,'2009-01-01T03:00',  " +
        " '2009-01-01T04:00' ,'2009-01-01T05:00', '2009-01-01T06:00'  " + 
        " )"
        ).orderBy("time").show();

        spark.close();
    }    
}
