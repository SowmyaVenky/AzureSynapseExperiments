package com.gssystems.spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.MessageFormat;

public class DownloadWeatherDataHistorical {
    public static String WEATHER_API = "https://archive-api.open-meteo.com/v1/era5?latitude={0}&longitude={1}&start_date={2}&end_date={3}&hourly=temperature_2m";
    //Home lat long 30.188530 -95.525810
    public static void main(String[] args ) throws IOException{
        if( args == null || args.length != 5) {
            System.out.println("Usage java DownloadWeatherDataHistorical lat long startdate enddate downloadfilename");
            System.exit(-1);
        }

        String lat = args[0];
        String lng = args[1];
        String stDate = args[2];
        String endDate = args[3];

        Object[] subs = {
            lat,
            lng,
            stDate,
            endDate
        };

        MessageFormat fmt = new MessageFormat(WEATHER_API);
        String apiURL = fmt.format(subs);
        System.out.println("Calling url " + apiURL);
        String response = stream(new URL(apiURL));
        System.out.println("Got response of length " + response.length());
        FileWriter fw = new FileWriter(args[4]);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(response);
        bw.flush();
        bw.close();
    }    

    public static String stream(URL url) throws IOException{
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
