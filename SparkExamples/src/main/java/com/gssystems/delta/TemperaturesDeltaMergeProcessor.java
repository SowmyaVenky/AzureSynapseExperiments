package com.gssystems.delta;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.delta.tables.DeltaTable;

public class TemperaturesDeltaMergeProcessor {
    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            System.out.println(
                    "Need to pass 2 parameters - directory of the delta main table + update file for this to work!");
            System.exit(-1);
        }

        String temperaturesDeltaDir = args[0];

        SparkSession spark = SparkSession.builder().appName("Temperatures Delta").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        DeltaTable table1 = DeltaTable.forPath(spark, temperaturesDeltaDir);

        Dataset<Row> updateRows = spark.read().json(args[1]);

        //This will make sure the temperatures field is twice as much as old.
        Dataset<Row> updateRows1 = updateRows.withColumn("temperature_2m", updateRows.col("temperature_2m").multiply(2.0));
        updateRows.show(5);
        updateRows1.show(5);

        Map<String,String> columnUpdatesToDo = new HashMap<String, String>();
        columnUpdatesToDo.put("olddata.temperature_2m", "newdata.temperature_2m");

        Map<String,String> columnInsertsToDo = new HashMap<String, String>();
        
        columnInsertsToDo.put("latitude", "newdata.latitude");
        columnInsertsToDo.put("longitude", "newdata.longitude");
        columnInsertsToDo.put("time", "newdata.time");
        columnInsertsToDo.put("temperature_2m", "newdata.temperature_2m");

        System.out.println("Merging the data to the main table...");

        table1.as("olddata").merge(updateRows1.as("newdata"), 
           " olddata.latitude = newdata.latitude " 
         + " and olddata.longitude = newdata.longitude " 
         + " and olddata.time = newdata.time ")
         .whenMatched().updateExpr(columnUpdatesToDo)
         .whenNotMatched().insertExpr(columnInsertsToDo)
         .execute();

        System.out.println("Done.");

        spark.close();
    }
}
