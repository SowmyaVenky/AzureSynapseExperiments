package com.gssystems.azeventhub;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Azure streaming analytics likes to have a proper timestamp to be able to
 * apply various types of window functions
 */
public class ASADateTimeAdjuster {
    private static final boolean WRITE_FILE_OUTPUTS = true;
    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            System.out.println("Need to pass 2 params, base parquet data and output directory to write");
            System.exit(-1);
        }

        SparkSession spark = SparkSession.builder().appName("Temperatures").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> inputDS = spark.read().parquet(args[0]);
        inputDS.printSchema(0);
        inputDS.show();

        System.out.println(inputDS.count());

        // Create a datetime field.
        Dataset<Row> dtAdded1 = inputDS
                .withColumn("year", org.apache.spark.sql.functions.substring(inputDS.col("time"), 1, 4))
                .withColumn("month", org.apache.spark.sql.functions.substring(inputDS.col("time"), 6, 2))
                .withColumn("day", org.apache.spark.sql.functions.substring(inputDS.col("time"), 9, 2))
                .withColumn("hour", org.apache.spark.sql.functions.substring(inputDS.col("time"), 12, 5));

        Dataset<Row> dtAdded = dtAdded1.withColumn("recorded_time", 
            org.apache.spark.sql.functions.to_timestamp(
                org.apache.spark.sql.functions.concat(
                    dtAdded1.col("year"),
                    org.apache.spark.sql.functions.lit("-"),
                    dtAdded1.col("month"),
                    org.apache.spark.sql.functions.lit("-"),
                    dtAdded1.col("day"),
                    org.apache.spark.sql.functions.lit(" "),
                    dtAdded1.col("hour"),
                    org.apache.spark.sql.functions.lit(":00")
                ), "yyyy-MM-dd HH:mm:ss"
            )
        );

        dtAdded.printSchema();
        dtAdded.show();
        
        if (WRITE_FILE_OUTPUTS) {
			System.out.println("Writing out the dataset with timestamp added...");
			dtAdded.repartition(1).write().parquet(args[1]);
		}

        spark.close();
    }
}
