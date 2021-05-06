package com.adrien.transform;

import com.adrien.sources.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMap_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.Source:从本地文件读取数据
        DataStream<String> streamSource = enev.readTextFile("/Users/luohaotian/Downloads/wordcountdemo.txt");
        DataStream<String> flatMapStream = streamSource.flatMap(new FlatMapFunction<String,String>() {
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(",");
                for (String field : fields){
                    out.collect(field);
                }
            }
        });
        streamSource.print();
        enev.execute();
    }
}
