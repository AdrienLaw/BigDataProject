package com.adrien.sink.kafka;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.streaming.connectors.redis.RedisSink;
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
//import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 完整的代码如下，实现一个读取Kafka的消息，然后进行WordCount，并把结果更新到redis中：
 */
public class FlinkSinkToRedis {
    public static void main(String[] args) throws Exception {
/*
        kafka 读取
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        enev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        enev.enableCheckpointing(2000);
        enev.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //连接kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop104:9092,hadoop105:9092");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("student-write", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        DataStream<String> kafkaStream = enev.addSource(consumer);
        DataStream<Tuple2<String, Integer>> counts = kafkaStream.flatMap(new LineSplitter()).keyBy(0).sum(1);
        */
        StreamExecutionEnvironment enev =StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = enev.socketTextStream("hadoop101", 9001);
        DataStream<String> filter = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (null == value || value.split(",").length != 2) {
                    return false;
                }
                return true;
            }
        });
        DataStream<Tuple2<String, String>> keyValue = filter.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], split[1]);
            }
        });
        //创建redis的配置 单机redis用FlinkJedisPoolConfig,集群redis需要用FlinkJedisClusterConfig
        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).setPassword("123123").build();

        keyValue.addSink(new RedisSink<Tuple2<String, String>>(redisConf, new RedisMapper<Tuple2<String, String>>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"table1");
            }
            @Override
            public String getKeyFromData(Tuple2<String, String> data) {
                return data.f0;
            }
            @Override
            public String getValueFromData(Tuple2<String, String> data) {
                return data.f1;
            }
        }));
        /*启动执行*/
        enev.execute();
    }
}


