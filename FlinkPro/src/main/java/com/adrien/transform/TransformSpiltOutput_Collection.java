package com.adrien.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;

/**
 * Spilt不能进行二级分流，我们用Side-Outputs进行二级分流，代码如下：
 */
public class TransformSpiltOutput_Collection {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
//        /*为方便测试 这里把并行度设置为1*/
//        enev.setParallelism(1);
//        // 1.Source:从本地文件读取数据
//        DataStreamSource<String> streamSource = enev.readTextFile("/Users/luohaotian/Downloads/person.txt");
//        SingleOutputStreamOperator<PersonInfo> mapStream = streamSource.map(new MapFunction<String, PersonInfo>() {
//            @Override
//            public PersonInfo map(String s) throws Exception {
//                String[] lines = s.split(",");
//                PersonInfo personInfo = new PersonInfo();
//                personInfo.setName(lines[0]);
//                personInfo.setProvince(lines[1]);
//                personInfo.setCity(lines[2]);
//                personInfo.setAge(Integer.valueOf(lines[3]));
//                personInfo.setIdCard(lines[4]);
//                return personInfo;
//            }
//        });
//        //定义流分类标识  进行一级分流
//        OutputTag<PersonInfo> shandongTag = new OutputTag<PersonInfo>("shandong") {};
//        OutputTag<PersonInfo> jiangsuTag = new OutputTag<PersonInfo>("jiangsu") {};
//        SingleOutputStreamOperator<Object> splitProvinceStream = mapStream.process(new ProcessFunction<PersonInfo, Object>() {
//            @Override
//            public void processElement(PersonInfo personInfo, Context context, Collector<Object> collector) throws Exception {
//                if ("shandong".equals(personInfo.getProvince())) {
//                    context.output(shandongTag, personInfo);
//                } else if ("jiangsu".equals(personInfo.getProvince())) {
//                    context.output(jiangsuTag, personInfo);
//                }
//            }
//        });
//        DataStream<PersonInfo> shandongStream = splitProvinceStream.getSideOutput(shandongTag);
//        DataStream<PersonInfo> jiangsuStream = splitProvinceStream.getSideOutput(jiangsuTag);
//        /*
//        *
//        * 下面对数据进行二级分流，我这里只对山东的这个数据流进行二级分流，江苏流程也一样
//        *
//        */
//        OutputTag<PersonInfo> jinanTag = new OutputTag<PersonInfo>("jinan") {};
//        OutputTag<PersonInfo> qingdaoTag = new OutputTag<PersonInfo>("qingdao") {};
//        SingleOutputStreamOperator<Object> shandongCityStream = shandongStream.process(new ProcessFunction<PersonInfo, Object>() {
//            @Override
//            public void processElement(PersonInfo personInfo, Context context, Collector<Object> collector) throws Exception {
//                if ("jinan".equals(personInfo.getCity())) {
//                    context.output(jinanTag, personInfo);
//                } else if ("qingdao".equals(personInfo.getCity())) {
//                    context.output(qingdaoTag, personInfo);
//                }
//            }
//        });
//        DataStream<PersonInfo> jinan = shandongCityStream.getSideOutput(jinanTag);
//        DataStream<PersonInfo> qingdao = shandongCityStream.getSideOutput(qingdaoTag);
//        jinan.map(new MapFunction<PersonInfo, String>() {
//            @Override
//            public String map(PersonInfo personInfo) throws Exception {
//                return personInfo.toString();
//            }
//        }).print("山东-济南二级分流结果:");
//        qingdao.map(new MapFunction<PersonInfo, String>() {
//            @Override
//            public String map(PersonInfo personInfo) throws Exception {
//                return personInfo.toString();
//            }
//        }).print("山东-青岛二级分流结果:");
//        enev.execute();
//    }
}
