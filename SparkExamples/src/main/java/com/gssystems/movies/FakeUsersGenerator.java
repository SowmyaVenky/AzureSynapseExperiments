package com.gssystems.movies;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.github.javafaker.Faker;

import org.apache.spark.api.java.function.MapFunction;

public class FakeUsersGenerator {
    private static final boolean WRITE_FILE_OUTPUTS = true;

    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            System.out.println("Need to pass 1 parameter - ratings.csv for this to work!");
            System.exit(-1);
        }

        String ratingsFile = args[0];
        SparkSession spark = SparkSession.builder().appName("Movielens Data processing").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        StructType ratingsSchema = new StructType(
                new StructField[] { new StructField("userId", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("movieId", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("rating", DataTypes.DoubleType, false, Metadata.empty()),
                        new StructField("timestamp", DataTypes.TimestampType, false, Metadata.empty())
                });

        Dataset<Row> ratingsdf = spark.read().option("header", "true").schema(ratingsSchema).csv(ratingsFile);

        Dataset<Row> usersDf = ratingsdf.select(ratingsdf.col("userId")
                .alias("user_id")).distinct();
        usersDf.printSchema();
        usersDf.show(10);

        StructType usersSchema = new StructType(
                new StructField[] {
                        new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("firstName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("lastName", DataTypes.StringType, false, Metadata.empty()),

                        new StructField("streetName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("number", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("city", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("state", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("zip", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("phone", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("creditCard", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("expDate", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("accountNumber", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("emailAddr", DataTypes.StringType, false, Metadata.empty())
                });

        ExpressionEncoder<Row> rowEnc = RowEncoder.apply(usersSchema);

        MapFunction<Row, Row> x1 = new MapFunction<Row, Row>() {
            @Override
            public Row call(Row value) throws Exception {
                Faker faker = new Faker();
                Integer userId = value.getAs("user_id");
                String firstName = faker.name().firstName();
                String lastName = faker.name().lastName();

                String streetName = faker.address().streetName();
                String number = faker.address().buildingNumber();
                String city = faker.address().city();
                String state = faker.address().state();
                String zip = faker.address().zipCode();

                String phone = faker.phoneNumber().cellPhone();
                String creditCard = faker.business().creditCardNumber();
                String expDate = faker.business().creditCardExpiry();
                String accountNumber = faker.commerce().promotionCode(10);

                String emailAddr = faker.bothify("????##@gmail.com");

                Object[] rowVals = new Object[] {
                        userId, firstName, lastName, streetName, number, city, state, zip, phone, creditCard, expDate,
                        accountNumber, emailAddr
                };

                Row toRet = RowFactory.create(rowVals);
                return toRet;
            }
        };
        Dataset<Row> fakeUsers = usersDf.map(x1, rowEnc);

        System.out.println("Showing fake users...");
        fakeUsers.show(10);

        if (WRITE_FILE_OUTPUTS) {
            System.out.println(fakeUsers.count() + " records, Writing users file");
            fakeUsers.write().mode(SaveMode.Overwrite).parquet("users");
        }

        spark.close();
    }

}
