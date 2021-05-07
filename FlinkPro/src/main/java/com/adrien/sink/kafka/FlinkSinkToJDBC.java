package com.adrien.sink.kafka;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

public class FlinkSinkToJDBC {
    public static void main(String[] args) throws Exception {
        // 解析参数
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if (parameterTool.getNumberOfParameters() < 4) {
            System.out.println("Missing parameters!");
            System.out.println("\nUsage: Kafka --topic <topic> " +
                    "--bootstrap.servers <kafka brokers> "+
                    "--zookeeper.connect <zk quorum> --group.id <some id>");
            return;
        }
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        enev.getConfig().disableSysoutLogging();
        enev.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,1000));
        enev.enableCheckpointing(5000);
        enev.getConfig().setGlobalJobParameters(parameterTool);
        // source
        DataStreamSource<String> streamSource = enev.addSource(
                new FlinkKafkaConsumer011<String>(parameterTool.getRequired("topic"),
                        new SimpleStringSchema(), parameterTool.getProperties()));
        // Transformation，这里仅仅是过滤了null。
        SingleOutputStreamOperator<Tuple3<String, String, String>> streamTransform = streamSource
                .map(new InputMap()).filter(new NullFilter());
        //sink
        streamTransform.addSink(new MySQLSink());
        enev.execute("Write into herSQL");
    }

    // 过滤Null数据。
    public static class NullFilter implements FilterFunction<Tuple3<String, String, String>> {
        @Override
        public boolean filter(Tuple3<String, String, String> value) throws Exception {
            return value != null;
        }
    }

    // 对输入数据做map操作。
    public static class InputMap implements MapFunction<String,Tuple3<String,String,String>> {
        @Override
        public Tuple3<String, String, String> map(String line) throws Exception {
            String[] splits = line.toLowerCase().split(",");
            if (splits.length > 2) {
                return new Tuple3<>(splits[0], splits[1], splits[2]);
            }
            return null;
        }
    }
}
