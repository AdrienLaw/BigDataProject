package com.adrien.udf;

import com.adrien.sources.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UdfCustomFilterFunction_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = enev.readTextFile("/Users/luohaotian/Downloads/SensorReading.txt");
        SingleOutputStreamOperator<SensorReading> mapStream = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String data) throws Exception {
                String[] splits = data.split(",");
                return new SensorReading(splits[0], Long.valueOf(splits[1].toString()), Double.valueOf(splits[2].toString()));
            }
        });
        SingleOutputStreamOperator<SensorReading> filterStream = mapStream.filter(new CustomFilterFunction());
        //filterStream .print("CustomFilterFunction");
        enev.execute("Function test");
    }
}
