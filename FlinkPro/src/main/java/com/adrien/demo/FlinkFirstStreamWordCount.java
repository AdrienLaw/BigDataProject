package com.adrien.demo;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkFirstStreamWordCount {
    public static void main(String[] args) throws Exception {
        //LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1);
        //StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("hadoop103", 6123, "/herPath/wordcount.jar");
        //ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment exev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputDataStream = exev.socketTextStream("hadoop101", 7778);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountDataSet = inputDataStream.flatMap(new FlinkFirstBatchWordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        wordCountDataSet.print().setParallelism(1);
        exev.execute();
    }
}
