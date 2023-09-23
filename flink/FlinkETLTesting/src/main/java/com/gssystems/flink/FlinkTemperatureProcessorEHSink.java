package com.gssystems.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkTemperatureProcessorEHSink {

	private static final String outputTopic = "temperatures";
	private static final String bootstrapServers = "venkyeh1001.servicebus.windows.net:9093";
	private static final String connectionString = "Endpoint=sb://venkyeh1001.servicebus.windows.net/;SharedAccessKeyName=venky-eh-sas;SharedAccessKey=NomtSaUnXt11invciDsYWwws0gs9FtvZn+AEhKjcv5o=;EntityPath=temperatures";

	public static void main(String[] args) throws Exception {
		// get environment context
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);

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

		// set properties.
		KafkaSinkBuilder<String> kafkaSinkBuilder = KafkaSink.<String>builder();
		kafkaSinkBuilder.setProperty("bootstrap.servers", bootstrapServers );
		kafkaSinkBuilder.setProperty("client.id", "FlinkTemperatureProcessor" );
		kafkaSinkBuilder.setProperty("sasl.mechanism", "PLAIN" );
		kafkaSinkBuilder.setProperty("security.protocol", "SASL_SSL" );
		kafkaSinkBuilder.setProperty("sasl.jaas.config", 
			"org.apache.kafka.common.security.plain.PlainLoginModule required " 
			+ "username=\"$ConnectionString\""
			+ "password=\"" 
			+ connectionString + "\";");

		// set serializer type.
		kafkaSinkBuilder.setRecordSerializer(KafkaRecordSerializationSchema.builder()
				.setTopic(outputTopic)
				.setValueSerializationSchema(new SimpleStringSchema())
				.build());

		KafkaSink<String> kafkaSink = kafkaSinkBuilder.build();
		
		env.execute("Temperature Processor Event Hub");
	}
}