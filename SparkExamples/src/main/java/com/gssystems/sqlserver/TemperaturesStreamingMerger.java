package com.gssystems.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TemperaturesStreamingMerger {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            System.out.println(
                    "Need to pass 2 parameters - kafka host and topic ");
            System.exit(-1);
        }

        String connectionURL = "jdbc:sqlserver://localhost:1433;DatabaseName=temperaturesdb;Encrypt=True;TrustServerCertificate=True";
        String userName = "sa";
        String pw = "Ganesh20022002";

        boolean showSchema = true;

        String serverToUse = args[0];
        String topic = args[1];
        int numLoopsWithZeroRows = 0;

        SparkSession spark = SparkSession.builder().appName("Temperatures Delta").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> mainData = spark.read()
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .format("jdbc")
                .option("url", connectionURL)
                .option("dbtable", "temperatures")
                .option("user", userName)
                .option("password", pw)
                .load();

        System.out.println("Reading the main table from SQLServer...");
        System.out.println("Read rows " + mainData.count());

        String bootstrapServers = serverToUse + ":9092";
        long offsetCounter = 0;

        while (true) {
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

            if (showSchema) {
                System.out.println("Raw schema from kafka read...");
                updateRows.printSchema(0);
            }

            System.out.println("Converting to a regular json df from value column");
            StructField[] fields = new StructField[] {
                    new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),
                    new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
                    new StructField("time", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("temperature_2m", DataTypes.DoubleType, false, Metadata.empty())
            };

            StructType schema = new StructType(fields);

            Dataset<Row> jsonDf = updateRows.select(org.apache.spark.sql.functions
                    .from_json(updateRows.col("value").cast("string"), schema).alias("value"));
            Dataset<Row> jsonDf1 = jsonDf
                    .select(
                            "value.latitude",
                            "value.longitude",
                            "value.time",
                            "value.temperature_2m");

            if (showSchema) {
                System.out.println("Schema after conversion from value to json dataset...");
                jsonDf1.printSchema(0);
            }

            long kafkaMessageCount = jsonDf1.count();
            System.out.println("Total number of rows :" + kafkaMessageCount);

            // Stop showing schema after first pass.
            showSchema = false;

            if (kafkaMessageCount == 0) {
                numLoopsWithZeroRows++;

                // Wait 1 min
                System.out.println("Waiting for 1 minute to see if messages come...");
                Thread.sleep(60 * 1000);

                // If there were 3 loops with np messages, then quit...
                if (numLoopsWithZeroRows >= 3) {
                    System.out.println("Waiting for 3 min for messages, aborting...");
                    break;
                }

                continue;
            }

            //Using the overwrite mode deletes off the old data.
            System.out.println("Storing the data from KAFKA into a temporary table...");
            jsonDf1.write().mode("overwrite")
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                    .format("jdbc")
                    .option("url", connectionURL)
                    .option("dbtable", "temperatures_kafka")
                    .option("user", userName)
                    .option("password", pw)
                    .save();

            System.out.println("Merging the data to the main table...");

            // Now call the merge inside SQL Server via a new connection...
            performSQLServerMerge(connectionURL, userName, pw);

            System.out.println("Waiting for 10 sec...");
            Thread.sleep(10000);

            System.out.println("Completed merge for current dataset from kafka of size: " + kafkaMessageCount);
            System.out.println("Done.");

            System.out.println("Before Updating  offset to new value = " + offsetCounter);
            offsetCounter = offsetCounter + kafkaMessageCount;
            System.out.println("Updating offset to new value = " + offsetCounter);
        }

        spark.close();
    }

    /* I know this sucks with no try/catch, but just for testing... */

    private static void performSQLServerMerge(String connectionURL,
            String user, String pw) throws Exception {
        System.out.println("Trying to connect to SQL Server");
        Connection conn = DriverManager.getConnection(connectionURL, user, pw);
        String mergeStatement = " MERGE temperatures AS TARGET " +
        " USING temperatures_kafka AS SOURCE " +
        " ON SOURCE.latitude = TARGET.latitude " +
        " AND SOURCE.longitude = TARGET.longitude " +
        " AND SOURCE.time = TARGET.time " +
        " WHEN NOT MATCHED BY TARGET THEN " +
        " INSERT (latitude, longitude, time, temperature_2m) " +
        " VALUES (SOURCE.latitude, SOURCE.longitude, SOURCE.time, SOURCE.temperature_2m) " +
        " WHEN MATCHED THEN UPDATE " +
        " SET TARGET.temperature_2m = SOURCE.temperature_2m; ";

        System.out.println(mergeStatement);
        
        PreparedStatement st = conn.prepareStatement(mergeStatement);
        boolean mergeResult = st.execute();
        System.out.println("Merge result from DB = " + mergeResult);
        st.close();
        conn.close();
    }
}
