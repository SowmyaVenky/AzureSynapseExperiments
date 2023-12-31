package com.gssystems.flink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.MessageFormat;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.google.gson.Gson;

class TemperatureDownloaderAndFormatter implements FlatMapFunction<String, String> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

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

	private String stream(URL url) throws IOException {
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
