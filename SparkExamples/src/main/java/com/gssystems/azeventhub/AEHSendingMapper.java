package com.gssystems.azeventhub;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.sql.Row;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.google.gson.Gson;

import scala.Function1;

/**
 * This program will take the data from the spark's Row object, pull the fields
 * we need and convert into a JSON payload that can be pushed to the event hub.
 * The code is not as efficient as it should be, since it is creating a batch of
 * one event.
 */
public class AEHSendingMapper implements Function1<Row, String>, Serializable {
    private static final String connectionString = Constants.CONNECTION_STRING;
    private transient EventHubProducerClient producer = null;
    private transient EventDataBatch eventDataBatch = null;
    private static int batchNumber = 0;
    private transient UUID clientID = null;

    public AEHSendingMapper() {
        startup();
    }

    private void startup() {
        producer = new EventHubClientBuilder()
                .connectionString(connectionString, "temperatures")
                .buildProducerClient();

        eventDataBatch = producer.createBatch();
        clientID = UUID.randomUUID();
    }

    @Override
    public String apply(Row v1) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();

        if (producer == null || eventDataBatch == null) {
            startup();
        }

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

        double temperature_f = (temperature_2m * 9 / 5) + 32.0;
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("latitude", latitude);
        dataMap.put("longitude", longitude);
        dataMap.put("time", time);
        dataMap.put("temperature_c", temperature_2m);
        dataMap.put("temperature_f", temperature_f);
        dataMap.put("year", year);
        dataMap.put("month", month);
        dataMap.put("day", day);
        dataMap.put("hour", hour);
        dataMap.put("recorded_time", ts);
        dataMap.put("generated_time", dtf.format(now));

        Gson gs = new Gson();
        toReturn = gs.toJson(dataMap, Map.class);

        EventData a = new EventData(toReturn);
        boolean addResult = eventDataBatch.tryAdd(a);
        if (addResult == false) {
            System.out.println("Failed to add into batch..." + toReturn);
        }

        if (eventDataBatch.getCount() % 1000 == 0) {
            producer.send(eventDataBatch);
            eventDataBatch = producer.createBatch();
            batchNumber++;
            System.out.println(clientID + " has sent 1000 events, batch number = " + batchNumber);
        }

        // Slow down the push so that we can do some streaming analytics
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return toReturn;
    }

    public void shutdown() {
        if (eventDataBatch.getCount() > 0) {
            producer.send(eventDataBatch);
        }

        producer.close();
    }
}
