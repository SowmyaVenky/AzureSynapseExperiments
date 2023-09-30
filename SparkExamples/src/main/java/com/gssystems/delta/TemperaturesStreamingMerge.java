package com.gssystems.delta;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.delta.tables.DeltaTable;

public class TemperaturesStreamingMerge {
    public static void main(String[] args) throws Exception {
        String serverToUse = "127.0.0.1";
        String topic = "temperatures";

        int numLoopsWithZeroRows = 0;

        if (args == null || args.length != 3) {
            System.out.println(
                    "Need to pass 3 parameters - directory of the delta main table + kafka host + topic to use for this to work!");
            System.exit(-1);
        }

        String temperaturesDeltaDir = args[0];
        serverToUse = args[1];
        topic = args[2];

        SparkSession spark = SparkSession.builder().appName("Temperatures Delta").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("Reading the delta table from disk...");
        DeltaTable table1 = DeltaTable.forPath(spark, temperaturesDeltaDir);

        String bootstrapServers = serverToUse + ":9092";
        long offsetCounter = 0;

        while( true ) {
            String startingOffset = "{\"temperatures\":{\"0\":" + offsetCounter + "}}";
            String endingOffset = "{\"temperatures\":{\"0\":-1}}";
            System.out.println("Reading the messsages from KAFKA topic " + bootstrapServers);
            Dataset<Row> updateRows = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("startingOffsets", startingOffset)
                .option("endingOffsets", endingOffset)
                .option("subscribe", topic)
                .option("kafka.group.id", "venky-cg-1")
                .load();

            System.out.println("Raw schema from kafka read...");
            updateRows.printSchema(0);

            System.out.println("Converting to a regular json df from value column");
            StructField[] fields = new StructField[] {
                new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),            
                new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("time", DataTypes.DateType, false, Metadata.empty()),
                new StructField("temperature_2m", DataTypes.DoubleType, false, Metadata.empty())
            };

            StructType schema = new StructType(fields);

            Dataset<Row> jsonDf = updateRows.select(org.apache.spark.sql.functions.from_json(updateRows.col("value").cast("string"), schema).alias("value"));
            Dataset<Row> jsonDf1 = jsonDf
            .select(
                "value.latitude",
                "value.longitude",
                "value.time",
                "value.temperature_2m"
            );

            System.out.println("Schema after conversion from value to json dataset...");
            jsonDf1.printSchema(0);

            long kafkaMessageCount = jsonDf1.count();
            System.out.println("Total number of rows :" + kafkaMessageCount);
            if( kafkaMessageCount == 0 ) {
                numLoopsWithZeroRows++;
                    
                //Wait 1 min
                System.out.println("Waiting for 1 minute to see if messages come...");
                Thread.sleep(60 * 1000 );
                
                //If there were 3 loops with np messages, then quit... 
                if( numLoopsWithZeroRows >= 3 ) {
                    System.out.println("Waiting for 3 min for messages, aborting...");
                    break;
                }

                continue;
            }

            offsetCounter = offsetCounter + kafkaMessageCount;
            
            Map<String,String> columnUpdatesToDo = new HashMap<String, String>();
            columnUpdatesToDo.put("olddata.temperature_2m", "newdata.temperature_2m");

            Map<String,String> columnInsertsToDo = new HashMap<String, String>();
            
            columnInsertsToDo.put("latitude", "newdata.latitude");
            columnInsertsToDo.put("longitude", "newdata.longitude");
            columnInsertsToDo.put("time", "newdata.time");
            columnInsertsToDo.put("temperature_2m", "newdata.temperature_2m");

            System.out.println("Merging the data to the main table...");

            table1.as("olddata").merge(jsonDf1.as("newdata"), 
            " olddata.latitude = newdata.latitude " 
            + " and olddata.longitude = newdata.longitude " 
            + " and olddata.time = newdata.time ")
            .whenMatched().updateExpr(columnUpdatesToDo)
            .whenNotMatched().insertExpr(columnInsertsToDo)
            .execute();

            System.out.println("Completed merge for current dataset from kafka of size: " + kafkaMessageCount);
            System.out.println("Done.");
        }

        spark.close();
    }
}
