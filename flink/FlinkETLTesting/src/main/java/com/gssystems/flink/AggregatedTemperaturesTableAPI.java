package com.gssystems.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class AggregatedTemperaturesTableAPI {
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		// create a DataStream
		DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");

		// interpret the insert-only DataStream as a Table
		Table inputTable = tableEnv.fromDataStream(dataStream);
		// register the Table object as a view and query it
		tableEnv.createTemporaryView("InputTable", inputTable);
		Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");

		// interpret the insert-only Table as a DataStream again
		DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

		// add a printing sink and execute in DataStream API
		resultStream.print();
		env.execute("Venky Table Test -- 1 ");
	}

}
