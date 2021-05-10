package com.adrien.transform;

import com.adrien.sources.SensorReading2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 一级分流
 */
public class TransformSpiltSelect_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.Source:从本地文件读取数据
        DataStreamSource<String> streamSource = enev.readTextFile("/Users/luohaotian/Downloads/person.txt");
        SingleOutputStreamOperator<PersonInfo> mapStream = streamSource.map(new MapFunction<String, PersonInfo>() {
            @Override
            public PersonInfo map(String s) throws Exception {
                String[] lines = s.split(",");
                PersonInfo personInfo = new PersonInfo();
                personInfo.setName(lines[0]);
                personInfo.setProvince(lines[1]);
                personInfo.setCity(lines[2]);
                personInfo.setAge(Integer.valueOf(lines[3]));
                personInfo.setIdCard(lines[4]);
                return personInfo;
            }
        });
        SplitStream<PersonInfo> splitStream = mapStream.split(new OutputSelector<PersonInfo>() {
            @Override
            public Iterable<String> select(PersonInfo personInfo) {
                ArrayList<String> splits = new ArrayList<>();
                if ("shandong".equals(personInfo.getProvince())) {
                    splits.add("shandong");
                } else if ("jiangsu".equals(personInfo.getProvince())) {
                    splits.add("jiangsu");
                }
                return splits;
            }
        });

        DataStream<PersonInfo> shandong = splitStream.select("shandong");
        DataStream<PersonInfo> jiangsu = splitStream.select("jiangsu");
        /*一级分流结果*/
        shandong.map(new MapFunction<PersonInfo, Object>() {
            @Override
            public Object map(PersonInfo personInfo) throws Exception {
                return personInfo.toString();
            }
        }).print("山东一级分流");

        jiangsu.map(new MapFunction<PersonInfo, Object>() {
            @Override
            public Object map(PersonInfo personInfo) throws Exception {
                return personInfo.toString();
            }
        }).print("江苏一级分流");
        enev.execute();
    }
}
