package com.gssystems.azeventhub;

import java.time.Duration;

import org.apache.spark.eventhubs.EventHubsConf;
import org.apache.spark.eventhubs.EventPosition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class AEHStreamToJSONDownloader {
    //Most insecure way of doing things :-)
    private static final String connectionString = Constants.CONNECTION_STRING;

    public static void main(String[] args) throws Exception {
        String outputDiretory = "eh_json_downloaded";

        if( args != null && args.length == 1 ) {
            outputDiretory = args[0];
        }
        else {
            System.out.println("Error pass kafka host ip and topic name");
            System.exit(-1);
        }

        SparkSession spark = SparkSession.builder()
            .appName("Temperatures_JSON")
            .getOrCreate();

        EventHubsConf ehconf = new EventHubsConf(connectionString);
        ehconf.setStartingPosition(EventPosition.fromStartOfStream());
                ehconf.setConsumerGroup("json-dw-cg");
        ehconf.setMaxEventsPerTrigger(10000);
        ehconf.setReceiverTimeout(Duration.ofSeconds(30));
  
		spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark
            .readStream()
            .format("eventhubs")
            .options(ehconf.toMap())
            .load();
        
        StructField[] fields = new StructField[] {
            new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),            
            new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("time", DataTypes.StringType, false, Metadata.empty()),
            new StructField("temperature_2m", DataTypes.DoubleType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);

        Dataset<Row> jsonDf = df.select(org.apache.spark.sql.functions.from_json(df.col("body").cast("string"), schema).alias("value"));
        Dataset<Row> jsonDf1 = jsonDf
        .withColumn("year", org.apache.spark.sql.functions.substring(jsonDf.col("value.time"),1,4))
        .withColumn("month", org.apache.spark.sql.functions.substring(jsonDf.col("value.time"),6,2))
        .withColumn("day", org.apache.spark.sql.functions.substring(jsonDf.col("value.time"),9,2))
        .withColumn("hour", org.apache.spark.sql.functions.substring(jsonDf.col("value.time"),12,5))
        .select(
            "year",
            "month",
            "day",
            "hour",
            "value.latitude",
            "value.longitude",
            "value.time",
            "value.temperature_2m"
        );

        //Flush every 2 mins
        Trigger tr = Trigger.ProcessingTime(120000);

        jsonDf1.writeStream()
        .outputMode("append")
        .format("json")
        .option("path", outputDiretory)
        .trigger(tr)
        .start();
        
        //Wait indefinitely!
        spark.streams().awaitAnyTermination();
    }

}
