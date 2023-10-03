package com.gssystems.azeventhub;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import scala.Function1;

/**
 * This program will take the data from the spark's Row object, pull the fields
 * we need and convert into a JSON payload that can be pushed to the event hub
 */
public class AEHSendingMapper implements Function1<Row, String>, Serializable {
    @Override
    public String apply(Row v1) {
        String toReturn = "";
        double latitude = v1.getAs("latitude");
        double longitude = v1.getAs("longitude");
        String time = v1.getAs("time");
        double temperature_2m = v1.getAs("temperature_2m");
        String year = v1.getAs("year");
        String month = v1.getAs("month");
        String day = v1.getAs("day");
        String hour = v1.getAs("hour");
        Timestamp ts = v1.getAs("recorded_time");

        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("latitude", latitude);
        dataMap.put("longitude", longitude);
        dataMap.put("time", time);
        dataMap.put("temperature_wm", temperature_2m);
        dataMap.put("year", year);
        dataMap.put("month", month);
        dataMap.put("day", day);
        dataMap.put("hour", hour);
        dataMap.put("recorded_time", ts);
        
        Gson gs = new Gson();
        toReturn = gs.toJson(dataMap, Map.class);
        return toReturn;
    }
}
