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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.types.Row;

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
		System.out.println("Printing the table from stream...");
		Expression selExpr1 = new SqlCallExpression("JSON_VALUE(f0, '$.lat')");
		Expression selExpr2 = new SqlCallExpression("JSON_VALUE(f0, '$.lng')");
		Expression selExpr3 = new SqlCallExpression("JSON_VALUE(f0, '$.year')");
		Expression selExpr4 = new SqlCallExpression("JSON_VALUE(f0, '$.month')");
		Expression selExpr5 = new SqlCallExpression("JSON_VALUE(f0, '$.count')");
		Expression selExpr6 = new SqlCallExpression("JSON_VALUE(f0, '$.minTemp')");
		Expression selExpr7 = new SqlCallExpression("JSON_VALUE(f0, '$.maxTemp')");
		table1.select(selExpr1, selExpr2, selExpr3, selExpr4, selExpr5, selExpr6, selExpr7)
				.as("longitude", "year", "month", "count", "min_temp", "max_temp").execute().print();
		table1.printSchema();

		Table inputTable = tableEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
				DataTypes.FIELD("name", DataTypes.STRING())), Row.of(1, "ABC"), Row.of(2L, "ABCDE"));

		System.out.println("Printing the table from hardcoded...");
		inputTable.execute().print();
	}

}
