package com.gssystems.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

public class TemperaturesDownloaderAndFormatter implements FlatMapFunction<Row, String> {
    private static final long serialVersionUID = 1L;

    public static String WEATHER_API = "https://archive-api.open-meteo.com/v1/era5?latitude={0}&longitude={1}&start_date={2}&end_date={3}&hourly=temperature_2m";

    private String startDate;
    private String endDate;

    public TemperaturesDownloaderAndFormatter(String stDate, String enDate) {
        this.startDate = stDate;
        this.endDate = enDate;
    }

    @Override
    public Iterator<String> call(Row t) throws Exception {
        ArrayList<String> toRet = new ArrayList<String>();
        // Get the lat and lng from the input row.
        String lat = t.getAs("lat");
        String lng = t.getAs("lng");
        if (lat != null && lng != null) {
            // Process the weather API for this row...
            Gson gs = new Gson();
            System.out.println("Lat " + lat + ", lng " + lng + ", startDate " + startDate + ", endDate " + endDate);

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
                        toRet.add(outJson);
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return toRet.iterator();
    }

    private String stream(URL url) throws Exception {
		try (InputStream input = url.openStream()) {
			InputStreamReader isr = new InputStreamReader(input);
			BufferedReader reader = new BufferedReader(isr);
			StringBuilder json = new StringBuilder();
			int c;
			while ((c = reader.read()) != -1) {
				json.append((char) c);
			}
            //causes 429 errors because it is too fast...
            Thread.sleep(1000);
			return json.toString();
		}
	}
}
