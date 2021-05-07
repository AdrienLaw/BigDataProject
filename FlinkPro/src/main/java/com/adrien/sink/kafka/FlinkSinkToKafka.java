package com.adrien.sink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class FlinkSinkToKafka {
    private static final String READ_TOPIC = "student";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092,hadoop104:9092,hadoop105:9092");
        props.put("zookeeper.connect", "hadoop101:2181,hadoop102:2181,hadoop103:2181");
        props.put("group.id", "student-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaStream = enev.addSource(new FlinkKafkaConsumer011<>(
                READ_TOPIC,  //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1);
        kafkaStream.print();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop104:9092,hadoop105:9092");
        properties.setProperty("zookeeper.connect", "hadoop101:2181,hadoop102:2181,hadoop103:2181");
        properties.setProperty("group.id", "student-write");
        kafkaStream.addSink(new FlinkKafkaProducer011<>(
                "hadoop102:9092,hadoop104:9092,hadoop105:9092",
                "student-write",
                new SimpleStringSchema()
        )).name("flink-connectors-kafka")
        .setParallelism(5);
        enev.execute();
    }
}
