package com.gssystems.azeventhub;

import java.io.BufferedReader;
import java.io.FileReader;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;

public class TemperaturesProducer {
    //Most insecure way of doing things :-)
    private static final String connectionString = Constants.CONNECTION_STRING;
    public static void main(String[] args) throws Exception {
        if( args == null || args.length != 2 ) {
            System.out.println("Usage TemperaturesProducer <<reformated temp file>> <<reformatted_loc_file>>");
            System.exit(-1);
        }

        publishEvents(args);
    }

    /**
     * Code sample for publishing events.
     * @throws IllegalArgumentException if the EventData is bigger than the max batch size.
     */
    public static void publishEvents(String[] args) throws Exception {
        // create a producer client
        EventHubProducerClient producer = new EventHubClientBuilder()
            .connectionString(connectionString, "temperatures")
            .buildProducerClient();

        //Read each row and send to topic.
        FileReader fr = new FileReader(args[0]);
        BufferedReader br = new BufferedReader(fr);
        String tempLine = null;
        EventDataBatch eventDataBatch = producer.createBatch();

        int x = 0;

        while((tempLine = br.readLine()) != null ) {
            EventData a = new EventData(tempLine);
            boolean addResult = eventDataBatch.tryAdd(a);
            if( addResult == false) {
                System.out.println("Failed to add into batch..." + tempLine);
                continue;
            }
            System.out.println("Sent " + tempLine);
            //Slow down the push so that we can do some streaming analytics
            Thread.sleep(10);
            x++;
            if( x != 0 && x % 1000 == 0 ) {
                if (eventDataBatch.getCount() > 0) {
                    producer.send(eventDataBatch);
                    eventDataBatch = producer.createBatch();
                }
            }
        }
        
        br.close();
        // send the last batch of remaining events
        if (eventDataBatch.getCount() > 0) {
            producer.send(eventDataBatch);
        }
        producer.close();
    }
}
