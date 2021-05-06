package com.adrien.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkFirstStreamWordCountOnLinux {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment exev = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> inputDataStream = exev.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountDataSet = inputDataStream.flatMap(new FlinkFirstBatchWordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        wordCountDataSet.print().setParallelism(1);
        exev.execute();
    }
}
