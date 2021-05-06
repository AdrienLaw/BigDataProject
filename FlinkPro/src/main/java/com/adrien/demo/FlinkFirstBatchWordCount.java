package com.adrien.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlinkFirstBatchWordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment exev = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String inPath = "/Users/luohaotian/Downloads/wordcountdemo.txt";
        DataSource<String> inpathDataSet = exev.readTextFile(inPath);
        // 空格分词打散之后，对单词进行 groupby 分组，然后用 sum 进行聚合
        DataSet<Tuple2<String, Integer>> wordCountDataSet = inpathDataSet
                .flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);
        wordCountDataSet.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String,Tuple2<String,Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(",");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word,1));
            }
        }
    }
}


