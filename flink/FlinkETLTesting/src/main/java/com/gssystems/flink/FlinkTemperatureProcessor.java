package com.gssystems.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTemperatureProcessor {
	public static void main(String[] args) throws Exception {		
		// get environment context
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);


		if( params == null || params.toMap().size() != 4 ) {
			System.out.println("Need to pass 2 parameters <<inputfile>> <<outputfolder>>");
			System.exit(-1);
		}

		final FileSink<String> sink = FileSink.forRowFormat(
				new Path(params.get("output")),
				new SimpleStringEncoder<String>("UTF-8")).build();

		// Build input stream
		final FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
				new Path(params.get("input")))
				.build();

		final DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
		TemperatureDownloaderAndFormatter x1 = new TemperatureDownloaderAndFormatter();
		String st = params.get("st");
		String end = params.get("end");

		System.out.println("Passed startDate : " + st);
		System.out.println("Passed endDate: " + end);

		x1.setStartDate(st);
		x1.setEndDate(end);

		DataStream<String> reformatted = stream.flatMap(x1);

		//Sink to the file system. 
		reformatted.sinkTo(sink).setParallelism(10);
		
		env.execute("Temperature Download FS");
	}
}