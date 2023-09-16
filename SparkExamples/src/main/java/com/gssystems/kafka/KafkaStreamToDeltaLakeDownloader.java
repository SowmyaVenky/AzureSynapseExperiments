package com.gssystems.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class KafkaStreamToDeltaLakeDownloader {
    public static void main(String[] args) throws Exception {
        String serverToUse = "127.0.0.1";
        String topic = "temperatures";
        String outputDiretory = "temperatures_delta";

        if( args != null && args.length == 3 ) {
            serverToUse = args[0];
            topic = args[1];
            outputDiretory = args[2];
        }
        else {
            System.out.println("Error pass kafka host ip and topic name");
            System.exit(-1);
        }

        String bootstrapServers = serverToUse + ":29092";

        SparkSession spark = SparkSession.builder()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .appName("Temperatures")
            .getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option("startingOffsets", "earliest")
            .option("subscribe", topic)
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

        jsonDf1.writeStream()
        .outputMode("append")
        .format("delta")
        .start(outputDiretory);
        
        //Wait indefinitely!
        spark.streams().awaitAnyTermination();
    }
}
