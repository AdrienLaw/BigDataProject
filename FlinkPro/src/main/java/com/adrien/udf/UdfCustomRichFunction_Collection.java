package com.adrien.udf;

import com.adrien.sources.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class UdfCustomRichFunction_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = enev.readTextFile("/Users/luohaotian/Downloads/AdminCode.txt");
        //3. Transformatioin转换
        //3.1 flatMap将一行字符串进行切分并压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream01 = dataStream.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] splits = s.split(",");
                for (int i = 0; i < splits.length; i++) {
                    collector.collect(Tuple2.of(splits[i], 1));
                }
            }
        });
        //3.2 对flatMap之后的数据进行过滤
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream02 = stream01.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> tuple2) throws Exception {
                if (tuple2.f0.startsWith("0")) {
                    return true;
                }
                return false;
            }
        });
        //3.3 对聚合后的结果进行map操作，关联出归属地名称
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> stream03 = stream02.map(new RichMapFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>>() {
            transient HashMap<String, String> hmgsd;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                hmgsd = new HashMap<>();
                hmgsd.put("010", "北京");
                hmgsd.put("0571", "杭州市");
                hmgsd.put("0551", "合肥市");
                hmgsd.put("0991", "乌鲁木齐");
                hmgsd.put("0351", "太原市");
            }

            @Override
            public Tuple3<String, String, Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
                String gsdName = null;
                // 获取号码归属地
                for (String key : hmgsd.keySet()) {
                    if (tuple2.f0.startsWith(key)) {
                        gsdName = hmgsd.get(key);
                    }
                }
                if (gsdName == null) {
                    gsdName = "未知";
                }
                return Tuple3.of(tuple2.f0, gsdName, tuple2.f1);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        //3.4 对过滤后的数据进行分组统计
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> sumStream =
                stream03.keyBy(1).sum(2);
        //4 sink输出结果
        sumStream.print();
        //5. 执行程序
        enev.execute("RichMapFunctionReview");


    }
}
