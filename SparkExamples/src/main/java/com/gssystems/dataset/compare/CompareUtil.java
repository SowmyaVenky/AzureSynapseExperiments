package com.gssystems.dataset.compare;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.Seq;

public class CompareUtil {
    public static final boolean WRITE_FILE_OUTPUTS = true;

    public static void main(String[] args) {

        if (args == null || args.length != 4) {
            System.out.println(
                    "Need to pass 4 parameters - ORC Directory, Parquet Directory, keyCols, Differences Directory for this to work!");
            System.exit(-1);
        }

        String orcLeftDataset = args[0];
        String parquetRightDataset = args[1];
        String keyCols = args[2];
        String DiffDataset = args[3];

        System.out.println("Left ORC dataset is: " + orcLeftDataset);
        System.out.println("Right Parquet dataset is: " + parquetRightDataset);
        System.out.println("Join Columns: " + keyCols);

        SparkSession spark = SparkSession.builder().appName("Dataset Compare").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // these options are needed as the fields are quoted.
        Dataset<Row> leftORC = spark.read().orc(orcLeftDataset);
        Dataset<Row> rightParquet = spark.read().parquet(parquetRightDataset);

        // Let us rename the columns on the right dataset so there are no name
        // conflicts. Ignore the key columns since they are going to be used in an
        // expression
        String[] keysToConsider = keyCols.split(",");
        List<String> keysArrList = Arrays.asList(keysToConsider);

        StructType rSchema = rightParquet.schema();
        String[] fieldNames = rSchema.fieldNames();
        for (String aCol : fieldNames) {
            if (!keysArrList.contains(aCol)) {
                rightParquet = rightParquet.withColumnRenamed(aCol, aCol + "_r");
            }
        }

        System.out.println("Left Dataset Schema");
        leftORC.printSchema();

        System.out.println("Right Dataset Schema");
        rightParquet.printSchema();

        System.out.println("Do Counts match = " + (leftORC.count() == rightParquet.count()));

        // Dynamically create the join sequence.
        List<String> joinExprArr = new ArrayList<String>();

        if (keysToConsider.length > 0) {
            for (int x = 0; x < keysToConsider.length; x++) {
                String aKey = keysToConsider[x];
                joinExprArr.add(aKey);
            }
        }

        // Create joined dataset.
        Seq<String> joinExprs = scala.collection.JavaConverters.asScalaBuffer(joinExprArr);
        System.out.println(joinExprs);
        Dataset<Row> joinedDataset = leftORC.alias("leftORC").join(rightParquet.alias("rightParquet"), joinExprs,
                "fullouter");
        joinedDataset.printSchema();
        joinedDataset.show();

        StructType structType = new StructType();
        structType = structType.add("leftDatasetName", DataTypes.StringType, false);
        structType = structType.add("rightDatasetName", DataTypes.StringType, false);
        structType = structType.add("columnName", DataTypes.StringType, false);
        structType = structType.add("leftValue", DataTypes.StringType, false);
        structType = structType.add("rightValue", DataTypes.StringType, false);
        structType = structType.add("matches", DataTypes.BooleanType, false);

        ExpressionEncoder<Row> rowEnc = RowEncoder.apply(structType);
        DifferencesGenerator x1 = new DifferencesGenerator(keysToConsider, orcLeftDataset, parquetRightDataset);
        Dataset<Row> diffDS = joinedDataset.flatMap(x1,
                rowEnc);
        System.out.println("Total Records : " + diffDS.count());

        diffDS.printSchema(0);
        diffDS.show();

        Dataset<Row> mismatchDS = diffDS.filter(diffDS.col("matches").equalTo(false));
        System.out.println("Total MISMATCHES Records : " + mismatchDS.count());


        if (WRITE_FILE_OUTPUTS) {
            diffDS.repartition(1).write().option("header", "true").mode(SaveMode.Overwrite).csv(DiffDataset);
        }
        spark.close();
    }

}
