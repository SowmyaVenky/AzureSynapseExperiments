package com.gssystems.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.SqlCallExpression;

/**
 * This program will take the aggregated JSONs we produced before, and use Table
 * API to parse and create a table that we can interact with way easily than
 * traditional java programming
 */
public class AggregatedTemperaturesTableAPI {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);

		// Build input stream
		final FileSource<String> source = FileSource
				.forRecordStreamFormat(new TextLineInputFormat(), new Path(params.get("input"))).build();
		final DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		Table table1 = tableEnv.fromDataStream(stream);

		Expression selExpr1 = new SqlCallExpression("JSON_VALUE(f0, '$.lat')");
		Expression selExpr2 = new SqlCallExpression("JSON_VALUE(f0, '$.lng')");
		Expression selExpr3 = new SqlCallExpression("JSON_VALUE(f0, '$.year')");
		Expression selExpr4 = new SqlCallExpression("JSON_VALUE(f0, '$.month')");
		Expression selExpr5 = new SqlCallExpression("JSON_VALUE(f0, '$.count')");
		Expression selExpr6 = new SqlCallExpression("JSON_VALUE(f0, '$.minTemp')");
		Expression selExpr7 = new SqlCallExpression("JSON_VALUE(f0, '$.maxTemp')");

		System.out.println("Printing the table after reformatting...");

		Table jsonParsedTable = table1.select(selExpr1, selExpr2, selExpr3, selExpr4, selExpr5, selExpr6, selExpr7)
				.as("latitude", "longitude", "year", "month", "count", "min_temp", "max_temp");
		jsonParsedTable.execute().print();
		jsonParsedTable.printSchema();

		// Now run aggregations on that table
		System.out.println("Generating aggregations on top of parsed table...");

		jsonParsedTable.groupBy(Expressions.$("year"), Expressions.$("month"))
				.select(Expressions.$("year"), Expressions.$("month"), Expressions.$("min_temp").min().as("minimum"),
						Expressions.$("max_temp").max().as("maximum"))
				.execute().print();

		// Now let us see whether we can use DDL to declare tables pointing to the file
		// system path. This uses the flink-json library. This makes it so much like how we do it on Hive etc.
		tableEnv.createTemporaryTable("source",
				TableDescriptor.forConnector("filesystem")
						.schema(Schema.newBuilder().column("lat", DataTypes.DOUBLE())
								.column("lng", DataTypes.DOUBLE())
								.column("year", DataTypes.INT())
								.column("month", DataTypes.INT())
								.column("count", DataTypes.DOUBLE())
								.column("minTemp", DataTypes.DOUBLE())
								.column("maxTemp", DataTypes.DOUBLE())								
								.build())
						.option("path", params.get("input")).format("json").build());

		Table definedTable = tableEnv.from("source");
		definedTable.execute().print();

		System.out.println("Executing direct query from tableEnv...");
		//To issue SQL based queries directly on the tables, we need to go to the tableEnv not table. 
		TableResult tableResult2 = tableEnv.sqlQuery("SELECT lat, lng, min(minTemp) as lowest, max(maxTemp) as highest"
				+ " FROM source group by lat, lng").execute();
		tableResult2.print();
		System.out.println("Table created...");
	}
}
