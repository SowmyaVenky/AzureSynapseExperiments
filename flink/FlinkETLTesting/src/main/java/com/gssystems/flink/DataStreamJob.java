package com.gssystems.flink;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.MessageFormat;

import com.google.gson.Gson;

class TemperatureDownloaderAndFormatter implements FlatMapFunction<String, String> {
	public static String WEATHER_API = "https://archive-api.open-meteo.com/v1/era5?latitude={0}&longitude={1}&start_date={2}&end_date={3}&hourly=temperature_2m";

	private String startDate;
	private String endDate;

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	@Override
	public void flatMap(String value, Collector<String> out) {
		// The input string contains a complex json.
		// We need to parse it to get arrays...
		Gson gs = new Gson();
		TemperatureDownloadInputBean inBean = gs.fromJson(
			value, TemperatureDownloadInputBean.class);

		Double lat = inBean.getLat();
		Double lng = inBean.getLng();

		System.out.println(value);
		System.out.println("Lat " + lat + ", lng" + lng + ", startDate " + startDate + ", endDate " + endDate);

		Object[] subs = {
				lat,
				lng,
				startDate,
				endDate
		};

		MessageFormat fmt = new MessageFormat(WEATHER_API);
		String apiURL = fmt.format(subs);
		System.out.println("Calling url " + apiURL);
		
		String response = null;
		try {
			response = stream(new URL(apiURL));
			if (response != null && response.length() > 0) {
				TemperaturesBean aBean = gs.fromJson(response, TemperaturesBean.class);
				double latitude = aBean.getLatitude();
				double longitude = aBean.getLongitude();

				List<String> times = aBean.getHourly().getTime();
				List<Double> temperatures = aBean.getHourly().getTemperature_2m();

				for (int x = 0; x < times.size(); x++) {
					TemperaturesOutBean outBean = new TemperaturesOutBean();
					outBean.setLatitude(latitude);
					outBean.setLongitude(longitude);
					outBean.setTime(times.get(x));
					outBean.setTemperature_2m(temperatures.get(x));

					String outJson = gs.toJson(outBean);
					out.collect(outJson);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private  String stream(URL url) throws IOException {
		try (InputStream input = url.openStream()) {
			InputStreamReader isr = new InputStreamReader(input);
			BufferedReader reader = new BufferedReader(isr);
			StringBuilder json = new StringBuilder();
			int c;
			while ((c = reader.read()) != -1) {
				json.append((char) c);
			}
			return json.toString();
		}
	}
}

public class DataStreamJob {
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

		reformatted.sinkTo(sink).setParallelism(10);

		env.execute("Aggreation");
	}
}