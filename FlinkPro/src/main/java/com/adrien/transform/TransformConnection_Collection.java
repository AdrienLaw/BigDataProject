package com.adrien.transform;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * 将两个不区分数据类型的数据流合并成一个数据流，并打印
 */
public class TransformConnection_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        enev.setParallelism(1);
        DataStreamSource<Tuple3<String, String, Integer>> dataStream01 = enev.fromCollection(
                Arrays.asList(
                        new Tuple3<>("张三", "man", 20),
                        new Tuple3<>("李四", "girl", 24),
                        new Tuple3<>("王五", "man", 29),
                        new Tuple3<>("刘六", "girl", 32),
                        new Tuple3<>("伍七", "girl", 18),
                        new Tuple3<>("吴八", "man", 30)
                )
        );
        DataStreamSource<Tuple3<String, String, Integer>> dataStream02 = enev.fromCollection(
                Arrays.asList(
                        new Tuple3<>("医生", "上海", 2),
                        new Tuple3<>("老师", "北京", 4),
                        new Tuple3<>("工人", "广州", 9)
                )
        );
        //合关两个数据流
        DataStream<Tuple4<String, String, Integer, String>> connectStream = dataStream01.connect(dataStream02)
                .map(new CoMapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, Tuple4<String, String, Integer, String>>() {
                    //表示dataStream1的流输入
                    @Override
                    public Tuple4<String, String, Integer, String> map1(Tuple3<String, String, Integer> value) throws Exception {
                        return Tuple4.of(value.f0, value.f1, value.f2, "用户");
                    }

                    //表示dataStream2的流输入
                    @Override
                    public Tuple4<String, String, Integer, String> map2(Tuple3<String, String, Integer> value) throws Exception {
                        return Tuple4.of(value.f0, value.f1, value.f2, "职业");
                    }
                });
        connectStream.print();
        enev.execute("flink Split job");
    }
}
