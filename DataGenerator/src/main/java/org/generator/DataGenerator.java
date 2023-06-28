package org.generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class DataGenerator {
    private static final String TOPIC_NAME = "input-topic"; // Replace with your Kafka topic name

    public static void main(String[] args) {
        // Set Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker addresses
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Generate and send data
            for (int i = 1; i==1; i++) {
                Long time =null;


//                String message = "{\n" +
//                        "    \"deviceId\": \"0\",\n" +
//                        "    \"sessionId\": \"0\",\n" +
//                        "    \"component\": [\n" +
//                        "        {\n" +
//                        "            \"code\": \"fhr1\",\n" +
//                        "            \"hr\": [\n" +
//                        "                200,\n" +
//                        "                201,\n" +
//                        "                202,\n" +
//                        "                203\n" +
//                        "            ]\n" +
//                        "        },\n" +
//                        "        {\n" +
//                        "            \"code\": \"utrine\",\n" +
//                        "            \"value\": [\n" +
//                        "                101,\n" +
//                        "                102,\n" +
//                        "                103,\n" +
//                        "                104\n" +
//                        "            ]\n" +
//                        "        }\n" +
//                        "    ]\n" +
//                        "}";
                producer.send(new ProducerRecord<>(TOPIC_NAME, null, "{\"deviceId\":\"0\",\"sessionId\":\"0\",\"component\":[{\"code\":\"fhr1\",\"hr\":[200,201,202,203]},{\"code\":\"fhr2\",\"hr\":[200,201,202,203]},{\"code\":\"fhr3\",\"hr\":[200,201,202,203]},{\"code\":\"utrine\",\"value\":[101,102,103,104]}],\"kafka_start_time\":" + System.currentTimeMillis() + "}"));
                System.out.println(new ProducerRecord<>(TOPIC_NAME, null, "{\"deviceId\":\"0\",\"sessionId\":\"0\",\"component\":[{\"code\":\"fhr1\",\"hr\":[200,201,202,203]},{\"code\":\"fhr2\",\"hr\":[200,201,202,203]},{\"code\":\"fhr3\",\"hr\":[200,201,202,203]},{\"code\":\"utrine\",\"value\":[101,102,103,104]}],\"kafka_start_time\":\"" + System.currentTimeMillis() + "\"}"));
                Thread.sleep(3000); // Optional: Wait for 1 second between each message
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // Close the Kafka producer
            producer.close();
        }
    }
}

