package com.gssystems.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import com.google.gson.Gson;

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

		MapFunction<String, Row> x1 = new MapFunction<String, Row>() {
			private static final long serialVersionUID = -1385202325062265709L;

			@Override
			public Row map(String value) throws Exception {
				Gson gs = new Gson();
				TemperatureAggregateBean aBean = gs.fromJson(value, TemperatureAggregateBean.class);
				Row aRow = Row.of(aBean.getLat(), aBean.getLng(), aBean.getYear(), aBean.getMonth(), aBean.getCount(),
						aBean.getMinTemp(), aBean.getMaxTemp());
				return aRow;
			}
		};
		
		// Let us convert the JSON Stream into a stream of Row objects.
		final DataStream<Row> pojoStream = stream.map(x1);
		
		Schema tableSchema = Schema.newBuilder()
				.column("latitude", DataTypes.DOUBLE())
				.column("longitude", DataTypes.DOUBLE())
				.column("year", DataTypes.INT())
				.column("month", DataTypes.INT())
				.column("count", DataTypes.DOUBLE())
				.column("MinTemp", DataTypes.DOUBLE())
				.column("MaxTemp", DataTypes.DOUBLE())
				.build();
		
		CloseableIterator<Row> iter = pojoStream.executeAndCollect();
		while(iter.hasNext()) {
			Row nextRow = iter.next();
			System.out.println(nextRow);
		}

		
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		Table table1 = tableEnv.fromDataStream(pojoStream);
		System.out.println("Printing the table from stream...");
		table1.execute().print();
		table1.printSchema();

		Table inputTable = tableEnv.fromValues(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
				DataTypes.FIELD("name", DataTypes.STRING())), Row.of(1, "ABC"), Row.of(2L, "ABCDE"));

		System.out.println("Printing the table from hardcoded...");
		inputTable.execute().print();
	}

}
