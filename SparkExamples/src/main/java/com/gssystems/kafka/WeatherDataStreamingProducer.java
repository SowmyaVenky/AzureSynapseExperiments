package com.gssystems.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class WeatherDataStreamingProducer {
    private static final Logger log = LoggerFactory.getLogger(WeatherDataStreamingProducer.class);

    public static void main(String[] args) throws Exception {
        if( args == null || args.length != 2 ) {
            System.out.println("Usage WeatherDataStreamingProducer <<reformated temp file>> <<reformatted_loc_file>>");
            System.exit(-1);
        }

        log.info("I am a Kafka Producer");

        String bootstrapServers = "127.0.0.1:29092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Read each row and send to topic.
        FileReader fr = new FileReader(args[0]);
        BufferedReader br = new BufferedReader(fr);
        String tempLine = null;
        while((tempLine = br.readLine()) != null ) {
            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("temperatures", tempLine);

            // send data - asynchronous
            producer.send(producerRecord);
            System.out.println("Sent " + producerRecord);
            //Slow down the push so that we can do some streaming analytics
            Thread.sleep(1000);
        }
        
        br.close();

        // flush data - synchronous
        producer.flush();

        // flush and close producer
        producer.close();
    }
}