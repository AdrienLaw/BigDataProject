package com.adrien.udf;

import com.adrien.sources.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class UdfCustomLambdaFunction_Collection {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment enev = ExecutionEnvironment.getExecutionEnvironment();
        List<String> datas = Arrays.asList("aa,bb,ccc,dd,aa", "cc,dd,ee,ff,gg,aa");
        DataSource<String> dataStream = enev.fromCollection(datas);
        dataStream.flatMap((String input, Collector<String[]> collector) -> {
            collector.collect(input.split(","));
        })
                .returns(Types.OBJECT_ARRAY(Types.STRING))
                .flatMap((String[] words, Collector<Tuple2<String, Integer>> collector) -> {
                    Arrays.stream(words).map(word -> new Tuple2<>(word, 1)).forEach(collector::collect);
                })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .groupBy(0)
                .sum(1)
                .print();
    }
}
