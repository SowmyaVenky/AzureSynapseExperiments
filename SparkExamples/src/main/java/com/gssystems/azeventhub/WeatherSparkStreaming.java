package com.gssystems.azeventhub;

import java.time.Duration;

import org.apache.spark.eventhubs.EventHubsConf;
import org.apache.spark.eventhubs.EventPosition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class WeatherSparkStreaming {
    //Most insecure way of doing things :-)
    private static final String connectionString = Constants.CONNECTION_STRING;

    public static void main(String[] args) throws Exception {		
        SparkSession spark = SparkSession.builder().appName("Temperatures").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

        EventHubsConf ehconf = new EventHubsConf(connectionString);
        ehconf.setStartingPosition(EventPosition.fromStartOfStream());

        ehconf.setConsumerGroup("stream-anal-cg");
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
            new StructField("time", DataTypes.DateType, false, Metadata.empty()),
            new StructField("temperature_2m", DataTypes.DoubleType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);

        Dataset<Row> jsonDf = df.select(
            org.apache.spark.sql.functions.from_json(df.col("body")
            .cast("string"), schema).alias("value")
        );
        
        //print schema
        jsonDf.printSchema(0);

        Dataset<Row> jsonDf1 = jsonDf.withColumn("year", org.apache.spark.sql.functions.substring(jsonDf.col("value.time"),1,4))
        .withColumn("month", org.apache.spark.sql.functions.substring(jsonDf.col("value.time"),6,2));

        jsonDf1.groupBy(
            "value.latitude",
            "value.longitude",
            "year",
            "month"
        ).agg( 
            org.apache.spark.sql.functions.count("value.temperature_2m").as("Measurements"),
            org.apache.spark.sql.functions.min("value.temperature_2m").as("Min_Temp"),
            org.apache.spark.sql.functions.max("value.temperature_2m").as("Max_Temp")
        ).writeStream().outputMode("complete").format("console").start();
        
        //Wait indefinitely!
        spark.streams().awaitAnyTermination();
    }
}
