package com.gssystems.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class WeatherSparkStreaming {
    public static void main(String[] args) throws Exception {		
		if (args == null || args.length != 2) {
			System.out.println("Need the host and topic name for this to work!");
			System.exit(-1);
		}

        System.out.println("USING KAFKA HOST " + args[0]);
        System.out.println("USING KAFKA TOPIC " + args[1]);

        SparkSession spark = SparkSession.builder().appName("Temperatures").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", args[0] + ":9092")
            .option("startingOffsets", "earliest")
            .option("subscribe", args[1])
            .load();
        
        StructField[] fields = new StructField[] {
            new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),            
            new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("time", DataTypes.DateType, false, Metadata.empty()),
            new StructField("temperature_2m", DataTypes.DoubleType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);

        Dataset<Row> jsonDf = df.select(org.apache.spark.sql.functions.from_json(df.col("value").cast("string"), schema).alias("value"));
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
