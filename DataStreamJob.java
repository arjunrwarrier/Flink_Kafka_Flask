package com.example;

import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
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

import java.util.ArrayList;
import java.util.List;

public class DataStreamJob {
    private static final Logger logger = LoggerFactory.getLogger(DataStreamJob.class);
    public static void Main() throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("172.20.243.130:9092").setTopics("input-topic").setGroupId("my-group").setStartingOffsets(OffsetsInitializer.latest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
        DataStream<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        sourceStream.print();
        DataStream<ObjectNode> jsonStream = sourceStream.map(value -> {
//            System.out.println("Time 1 : msg received from Kafka\t:\t" + System.currentTimeMillis());
            logger.info("Time 1 : msg received from Kafka\t" + System.currentTimeMillis());

            ObjectMapper mapper = new ObjectMapper();
            ObjectNode fhrData = mapper.createObjectNode();
            fhrData = (ObjectNode) mapper.readTree(value);
            return fhrData;
        });
        DataStream<String> fhr1 = jsonStream.flatMap(new FlatMapFunction<ObjectNode, String>() {
            @Override
            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {
                List<String> resultList = new ArrayList<>();
                String fhr1 = null, utrine = null;
                for (JsonNode component : jsonNodes.get("component")) {
                    String code = component.get("code").asText();
                    if (code.equals("fhr1")) {
                        fhr1=component.get("hr").toString();
                    }
                    if (code.equals("utrine")) {
                        utrine = component.get("value").toString();
//                        collector.collect(component.get("value").toString());
                    }
                }
                if(fhr1 != null && utrine != null){
                    resultList.add(fhr1);
                    resultList.add(utrine);
                    System.out.println("Processed to DataStream fhr1"+ System.currentTimeMillis());
                    collector.collect(resultList.toString());
                }
            }
        }).setParallelism(1);


        DataStream<String> fhr2 = jsonStream.flatMap(new FlatMapFunction<ObjectNode, String>() {
            @Override
            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {
                List<String> resultList = new ArrayList<>();
                String fhr2 = null, utrine = null;
                for (JsonNode component : jsonNodes.get("component")) {
                    String code = component.get("code").asText();
                    if (code.equals("fhr2")) {
                        fhr2=component.get("hr").toString();
                    }
                    if (code.equals("utrine")) {
                        utrine = component.get("value").toString();
//                        collector.collect(component.get("value").toString());
                    }
                }
                if(fhr2 != null && utrine != null){
                    resultList.add(fhr2);
                    resultList.add(utrine);
                    collector.collect(resultList.toString());
                    System.out.println("Processed to DataStream fhr2"+ System.currentTimeMillis());
                }
            }
        }).setParallelism(1);
        DataStream<String> fhr3 = jsonStream.flatMap(new FlatMapFunction<ObjectNode, String>() {
            @Override
            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {
                List<String> resultList = new ArrayList<>();
                String fhr3 = null, utrine = null;
                for (JsonNode component : jsonNodes.get("component")) {
                    String code = component.get("code").asText();
                    if (code.equals("fhr3")) {
                        fhr3=component.get("hr").toString();
                    }
                    if (code.equals("utrine")) {
                        utrine = component.get("value").toString();
//                        collector.collect(component.get("value").toString());
                    }
                }
                if(fhr3 != null && utrine != null){
                    resultList.add(fhr3);
                    resultList.add(utrine);
                    collector.collect(resultList.toString());
                    System.out.println("Processed to DataStream fhr3"+ System.currentTimeMillis());
                }
            }
        }).setParallelism(1);

//
//        DataStream<String> parentStream = jsonStream.flatMap(new FlatMapFunction<ObjectNode, String>() {
//            @Override
//            public void flatMap(ObjectNode jsonNodes, Collector<String> collector) throws Exception {
//                List<String> resultList = new ArrayList<>();
//                for (JsonNode component : jsonNodes.get("component")) {
//                    String code = component.get("code").asText();
//                    if (code.equals("utrine")) {
//                        resultList.add(component.get("value").toString());
//                    }
//                }
//                collector.collect(resultList.toString());
//            }
//        });

        DataStream<String> processedStream = fhr1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
//                System.out.println("Time 2 : FHR1 sending from flink\t:\t" + System.currentTimeMillis());
                logger.info("Time 2 : FHR1 sending from flink\t: " + System.currentTimeMillis());

                RequestSpecification request = RestAssured.given();
                request.header("Content-Type", "application/json");
                request.body(value);
                request.post("http://172.20.243.130:5001/");
//                System.out.println("Time 4 : predication FHR1 back to Flink\t:\t" + System.currentTimeMillis());
                logger.info("Time 4 : predication FHR1 back to Flink\t: " + System.currentTimeMillis());

                return "";

            }
        }).setParallelism(1);

        DataStream<String> processedStream2 = fhr2.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
//                System.out.println("Time 2 : FHR2 sending from flink\t:\t" + System.currentTimeMillis());
                logger.info("Time 2 : FHR2 sending from flink\t: " + System.currentTimeMillis());


                RequestSpecification request = RestAssured.given();
                request.header("Content-Type", "application/json");
                request.body(value);
                request.post("http://172.20.243.130:5002/");
//                System.out.println("Time 4 : predication FHR1 back to Flink\t:\t" + System.currentTimeMillis());
                logger.info("Time 4 : predication FHR2 back to Flink\t: " + System.currentTimeMillis());

                return "";

            }
        }).setParallelism(1);
        DataStream<String> processedStream3 = fhr3.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
//                System.out.println("Time 2 : FHR2 sending from flink\t:\t" + System.currentTimeMillis());
                logger.info("Time 2 : FHR3 sending from flink\t: " + System.currentTimeMillis());


                RequestSpecification request = RestAssured.given();
                request.header("Content-Type", "application/json");
                request.body(value);
                request.post("http://172.20.243.130:5003/");
//                System.out.println("Time 4 : predication FHR1 back to Flink\t:\t" + System.currentTimeMillis());
                logger.info("Time 4 : predication FHR3 back to Flink\t: " + System.currentTimeMillis());

                return "";

            }
        }).setParallelism(1);

        env.execute("SplitJob");

    }


}