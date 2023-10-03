package com.gssystems.azeventhub;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import scala.Function1;
import scala.runtime.BoxedUnit;

public class SparkEventHubProducer {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 1) {
            System.out.println("Need to pass 1 params, date time added parquet directory");
            System.exit(-1);
        }

        SparkSession spark = SparkSession.builder().appName("Temperatures").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> inputDS = spark.read().parquet(args[0]);
        inputDS.printSchema(0);
        inputDS.show();

        System.out.println(inputDS.count());
        System.out.println("Repartitioning the dataset so that each partition can be pushed to the event hub");

        AEHSendingMapper mapFn = new AEHSendingMapper();
        Dataset<String> outDS = inputDS.coalesce(1000).map(mapFn, Encoders.STRING());
        outDS.printSchema();
        System.out.println(outDS.count());
        outDS.show(10, false);

        spark.close();
    }
}
