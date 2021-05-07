package com.adrien.udf;

import com.adrien.sources.SensorReading;
import com.adrien.sources.SensorReading2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UdfCustomMapFunction_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = enev.readTextFile("/Users/luohaotian/Downloads/SensorReading.txt");
        // 1、先转换成SensorReading类型（简单转换操作）
        SingleOutputStreamOperator<SensorReading> stream01 = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String data) throws Exception {
                String[] splits = data.split(",");
                return new SensorReading(splits[0], Long.valueOf(splits[1].toString()), Double.valueOf(splits[2].toString()));
            }
        });
        // 调用自定义CustomMapFunction类的，转换输出
        SingleOutputStreamOperator<String> dataStreamMap = stream01.map(new CustomMapFunction());
        //dataStreamMap.print("CustomFilterFunction");
        enev.execute("Function test");
    }
}
