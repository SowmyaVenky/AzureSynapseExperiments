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
import org.apache.flink.types.Row;

public class AggregatedTemperaturesTableAPI {
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		Table inputTable = tableEnv.fromValues(
				DataTypes.ROW(
				        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
				        DataTypes.FIELD("name", DataTypes.STRING())
				    ),
				   Row.of(1, "ABC"),
				   Row.of(2L, "ABCDE")
		);
		
		inputTable.execute().print();
	}

}
