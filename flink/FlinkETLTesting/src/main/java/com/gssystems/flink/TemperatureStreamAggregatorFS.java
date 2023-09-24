package com.gssystems.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.google.gson.Gson;

public class TemperatureStreamAggregatorFS {
	public static void main(String[] args) throws Exception {
		// get environment context
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);

		if( params == null || params.toMap().size() != 2 ) {
			System.out.println("Need to pass 2 parameters <<inputfile>> <<outputfolder>>");
			System.exit(-1);
		}

		final FileSink<String> sink = FileSink.forRowFormat(
				new Path(params.get("output")),
				new SimpleStringEncoder<String>("UTF-8")).build();

	
		// Build input stream
		final FileSource<String> source = FileSource
				.forRecordStreamFormat(new TextLineInputFormat(), new Path(params.get("input"))).build();

		final DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

		TemperaturesAggregator x1 = new TemperaturesAggregator();
		SingleOutputStreamOperator<Tuple2<String, TemperatureAggregateBean>> aggregated = stream.map(x1);

		ReduceFunction<Tuple2<String, TemperatureAggregateBean>> reduceFn = new ReduceFunction<Tuple2<String, TemperatureAggregateBean>>() {
			private static final long serialVersionUID = 1317152878505343332L;

			@Override
			public Tuple2<String, TemperatureAggregateBean> reduce(Tuple2<String, TemperatureAggregateBean> value1,
					Tuple2<String, TemperatureAggregateBean> value2) throws Exception {
				TemperatureAggregateBean val1 = value1.f1;
				TemperatureAggregateBean val2 = value2.f1;

				TemperatureAggregateBean redBean = new TemperatureAggregateBean();
				redBean.setLat(val1.getLat());
				redBean.setLng(val1.getLng());
				redBean.setYear(val1.getYear());
				redBean.setMonth(val1.getMonth());

				double temp1 = val1.getMaxTemp();
				double temp2 = val2.getMaxTemp();

				redBean.setMaxTemp(Math.max(temp1, temp2));

				double minT1 = val1.getMinTemp();
				double minT2 = val2.getMinTemp();

				redBean.setMinTemp(Math.min(minT1, minT2));
				redBean.setCount(val1.getCount() + val2.getCount());

				String key = value1.f0;
				return new Tuple2<String, TemperatureAggregateBean>(key, redBean);
			}
		};

		SingleOutputStreamOperator<Tuple2<String, TemperatureAggregateBean>> minsByLatLng = aggregated.keyBy(TemperatureAggregateBean::getKey)
				.reduce(reduceFn);		
		
		FlatMapFunction<Tuple2<String, TemperatureAggregateBean>,String> x2 = new FlatMapFunction<Tuple2<String,TemperatureAggregateBean>, String>() {
			private static final long serialVersionUID = 9147274937506693902L;

			@Override
			public void flatMap(Tuple2<String, TemperatureAggregateBean> value, Collector<String> out) throws Exception {
				TemperatureAggregateBean aBean = value.f1;
				Gson gs = new Gson();
				out.collect(gs.toJson(aBean));			
			}
		};
		SingleOutputStreamOperator<String> reformatted = minsByLatLng.flatMap(x2);
		reformatted.sinkTo(sink).setParallelism(1);
		
		env.execute("Temperature Aggregator");
	}
}
