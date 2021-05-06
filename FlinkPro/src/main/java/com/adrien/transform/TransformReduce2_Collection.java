package com.adrien.transform;

import com.adrien.sources.SensorReading;
import com.adrien.sources.SensorReading2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduce2_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.Source:从本地文件读取数据
        DataStreamSource<String> streamSource = enev.readTextFile("/Users/luohaotian/Downloads/SensorReading.txt");
        // 转换成 SensorReading 类型
        SingleOutputStreamOperator<SensorReading2> dataStream = streamSource.map(new MapFunction<String, SensorReading2>() {
            public SensorReading2 map(String input) throws Exception {
                String[] fields = input.split(",");
                String id = fields[0];
                Long timestamp = Long.parseLong(fields[1]);
                Double temperatur = Double.parseDouble(fields[2]);
                return new SensorReading2(id, timestamp, temperatur);
            }
        });
        // 分组
        KeyedStream<SensorReading2, Tuple> keyedStream = dataStream.keyBy("id");
        // reduce 聚合，取最小的温度值，并输出当前的时间戳
        SingleOutputStreamOperator<SensorReading2> reduceStream = keyedStream.reduce(new ReduceFunction<SensorReading2>() {
            @Override
            public SensorReading2 reduce(SensorReading2 value1, SensorReading2 value2) throws Exception {
                return new SensorReading2(
                        value1.getId(),
                        value2.getTimestamp(),
                        Math.min(value1.getTemperature(), value2.getTemperature()));
            }
        });
        reduceStream.print();
        enev.execute();
    }
}
