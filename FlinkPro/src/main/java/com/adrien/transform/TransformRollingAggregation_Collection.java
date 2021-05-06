package com.adrien.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRollingAggregation_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.Source:从本地文件读取数据
        DataStream<String> streamSource = enev.readTextFile("/Users/luohaotian/Downloads/wordcountdemo.txt");
        SingleOutputStreamOperator<String> streamOperator = streamSource.filter(new FilterFunction<String>() {
            public boolean filter(String input) throws Exception {
                System.out.println(input.contains("粽子"));
                return input.contains("粽子");
            }
        });
        streamSource.print();
        enev.execute();
    }
}
