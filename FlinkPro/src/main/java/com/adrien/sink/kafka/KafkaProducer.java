package com.adrien.sink.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducer {
    private static final String broker_list = "hadoop102:9092,hadoop104:9092,hadoop105:9092";
    private static final String topic = "student";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        try {
            for (int i = 0; i <= 1000 ; i ++) {
                Student student = new Student(i, "itzzy" + i, "password" + i, 18 + i);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, null, JSON.toJSONString(student));
                kafkaProducer.send(record);
                System.out.println("发送数据: " + JSON.toJSONString(student));
            }
            Thread.sleep(3000);
        } catch (Exception e) {

        }
        kafkaProducer.flush();
    }
}
