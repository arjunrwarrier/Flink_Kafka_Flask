package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

public class DataStreamJob {
    private static final Logger logger = LoggerFactory.getLogger(DataStreamJob.class);

    public static void Main() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("localhost:9092").setTopics("input-topic").setGroupId("my-group").setStartingOffsets(OffsetsInitializer.latest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
        DataStream<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<ObjectNode> jsonStream = sourceStream.map(value -> {

            ObjectMapper mapper = new ObjectMapper();
            // Get the value of the "time" field
            ObjectNode fhrData = mapper.createObjectNode();
            fhrData = (ObjectNode) mapper.readTree(value);
            fhrData.put("kafka_receive_time", System.currentTimeMillis());
            return fhrData;
        }).setParallelism(1);
        jsonStream.flatMap(new FlatMapFunction<ObjectNode, String>() {
            @Override
            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {

                List<String> resultList = new ArrayList<>();
                String fhr1 = null, utrine = null, kafkaStartTime = null, kafkaRecieveTime = null;
                for (JsonNode component : jsonNodes.get("component")) {
                    String code = component.get("code").asText();
                    if (code.equals("fhr1")) {
                        fhr1 = component.get("hr").toString();
                    }
                    if (code.equals("utrine")) {
                        utrine = component.get("value").toString();
                    }

                }
                kafkaStartTime = jsonNodes.get("kafka_start_time").toString();
                kafkaRecieveTime = jsonNodes.get("kafka_receive_time").toString();

                if (fhr1 != null && utrine != null) {
                    resultList.add(fhr1);
                    resultList.add(utrine);
                    resultList.add("kafkaStartTime : " + kafkaStartTime);
                    resultList.add("kafkaEndTime : " + kafkaRecieveTime);
                    collector.collect(resultList.toString());
                }
            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<String> requestEntity = new HttpEntity<>(value, headers);
                // Send the POST request
                ResponseEntity<String> responseEntity=restTemplate.exchange("http://localhost:5001/", HttpMethod.POST, requestEntity, String.class);

                // Recieve the response
                String responseFhrData = responseEntity.getBody();
                // Parse the response data as JsonNode
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode responseJson = objectMapper.readTree(responseFhrData);

                // Add the current time to the response JSON
                ((ObjectNode) responseJson).put("Prediction_Received_FHR1_Time", System.currentTimeMillis());

                // Convert the modified JSON back to string
                String modifiedResponseData = objectMapper.writeValueAsString(responseJson);

                logger.info("FHR1 response data: " + modifiedResponseData);

                return "";

            }
        }).setParallelism(1);


        jsonStream.flatMap(new FlatMapFunction<ObjectNode, String>() {

            @Override
            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {
                List<String> resultList = new ArrayList<>();
                String fhr2 = null, utrine = null, kafkaStartTime = null, kafkaRecieveTime = null;
                for (JsonNode component : jsonNodes.get("component")) {
                    String code = component.get("code").asText();
                    if (code.equals("fhr2")) {
                        fhr2 = component.get("hr").toString();
                    }
                    if (code.equals("utrine")) {
                        utrine = component.get("value").toString();
                    }

                }
                kafkaStartTime = jsonNodes.get("kafka_start_time").toString();
                kafkaRecieveTime = jsonNodes.get("kafka_receive_time").toString();
                if (fhr2 != null && utrine != null) {
                    resultList.add(fhr2);
                    resultList.add(utrine);
                    resultList.add("kafkaStartTime : " + kafkaStartTime);
                    resultList.add("kafkaEndTime : " + kafkaRecieveTime);
                    collector.collect(resultList.toString());

                }
            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
//                logger.info("Time 2 : FHR2 sending from flink\t: " + System.currentTimeMillis());
                RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<String> requestEntity = new HttpEntity<>(value, headers);
                // Send the POST request
                ResponseEntity<String> responseEntity=restTemplate.exchange("http://localhost:5002/", HttpMethod.POST, requestEntity, String.class);

                String responseFhrData = responseEntity.getBody();
                // Parse the response data as JsonNode
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode responseJson = objectMapper.readTree(responseFhrData);

                // Add the current time to the response JSON
                ((ObjectNode) responseJson).put("Prediction_Received_FHR2_Time", System.currentTimeMillis());

                // Convert the modified JSON back to string
                String modifiedResponseData = objectMapper.writeValueAsString(responseJson);

                logger.info("FHR2 response data: " + modifiedResponseData);

                return "";

            }
        }).setParallelism(1);

        jsonStream.flatMap(new FlatMapFunction<ObjectNode, String>() {
            @Override
            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {
                List<String> resultList = new ArrayList<>();
                String fhr3 = null, utrine = null, kafkaStartTime = null, kafkaRecieveTime = null;
                for (JsonNode component : jsonNodes.get("component")) {
                    String code = component.get("code").asText();
                    if (code.equals("fhr3")) {
                        fhr3 = component.get("hr").toString();
                    }
                    if (code.equals("utrine")) {
                        utrine = component.get("value").toString();
                    }
                }
                kafkaStartTime = jsonNodes.get("kafka_start_time").toString();
                kafkaRecieveTime = jsonNodes.get("kafka_receive_time").toString();
                if (fhr3 != null && utrine != null) {
                    resultList.add(fhr3);
                    resultList.add(utrine);
                    resultList.add("kafkaStartTime : " + kafkaStartTime);
                    resultList.add("kafkaEndTime : " + kafkaRecieveTime);
                    collector.collect(resultList.toString());
                }
            }
        }).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
//                logger.info("Time 2 : FHR3 sending from flink\t: " + System.currentTimeMillis());

                RestTemplate restTemplate = new RestTemplate(new HttpComponentsClientHttpRequestFactory());
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<String> requestEntity = new HttpEntity<>(value, headers);
                // Send the POST request
                ResponseEntity<String> responseEntity=restTemplate.exchange("http://localhost:5003/", HttpMethod.POST, requestEntity, String.class);

                String responseFhrData = responseEntity.getBody();

                // Parse the response data as JsonNode
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode responseJson = objectMapper.readTree(responseFhrData);

                // Add the current time to the response JSON
                ((ObjectNode) responseJson).put("Prediction_Received_FHR3_Time", System.currentTimeMillis());

                // Convert the modified JSON back to string
                String modifiedResponseData = objectMapper.writeValueAsString(responseJson);

                logger.info("FHR3 response data: " + modifiedResponseData);

                return "";

            }
        }).setParallelism(1);

        env.execute("SplitJob");

    }


}
