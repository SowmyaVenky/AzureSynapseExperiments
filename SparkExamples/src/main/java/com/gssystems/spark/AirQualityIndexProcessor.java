package com.gssystems.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class AirQualityIndexProcessor {
    private static final boolean WRITE_FILE_OUTPUTS = true;
    private static final int MAX_CITIES_TO_PROCESS = 100;

    public static void main(String[] args) {

        if (args == null || args.length != 4) {
            System.out.println(
                    "Need to pass 4 parameters - directory of the AQI file, output folder, start and end date for this to work!");
            System.exit(-1);
        }

        String aqiFolder = args[0];

        SparkSession spark = SparkSession.builder().appName("Temperatures").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // these options are needed as the fields are quoted.
        Dataset<Row> tempsDF = spark.read().option("header", "true").csv(aqiFolder);
        tempsDF.printSchema();
        System.out.println("AQI Index row count..." + tempsDF.count());

        /*
         * This will read the lat and lng and respond back with a JSON of all the
         * temperatures for the given dates
         */
        String startDate = args[2];
        String endDate = args[3];

        StructType structType = new StructType();
        structType = structType.add("latitude", DataTypes.DoubleType, false);
        structType = structType.add("longitude", DataTypes.DoubleType, false);
        structType = structType.add("time", DataTypes.StringType, false);
        structType = structType.add("temperature_2m", DataTypes.DoubleType, false);

        ExpressionEncoder<Row> rowEnc = RowEncoder.apply(structType);

        TemperaturesDownloaderAndFormatter x1 = new TemperaturesDownloaderAndFormatter(startDate, endDate);
        Dataset<Row> temperaturesDS = tempsDF.limit(MAX_CITIES_TO_PROCESS).flatMap(x1,
                rowEnc);
        System.out.println(temperaturesDS.count());
        temperaturesDS.printSchema(0);

        if (WRITE_FILE_OUTPUTS) {
            temperaturesDS.write().parquet(args[1]);
        }
        spark.close();
    }

}
