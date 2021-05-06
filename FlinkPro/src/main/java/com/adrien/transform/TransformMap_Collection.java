package com.adrien.transform;

import com.adrien.sources.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMap_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.Source:从本地文件读取数据
        DataStreamSource<String> streamSource = enev.readTextFile("/Users/luohaotian/Downloads/SensorReading.txt");
        SingleOutputStreamOperator<SensorReading> resultDataStream = streamSource.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String input) throws Exception {
                String[] fields = input.split(",");
                String id = fields[0];
                Long timestamp = Long.parseLong(fields[1]);
                Double temperatur = Double.parseDouble(fields[2]);
                return new SensorReading(id, timestamp, temperatur);
            }
        });
        resultDataStream.print();
        enev.execute();
    }
}
