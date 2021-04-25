package com.adrien.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountJava {
    public static void main(String[] args) throws InterruptedException {
        //步骤一：初始化程序入口
        SparkConf sparkConf = new SparkConf().setAppName("WordCountJava").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        //步骤二：获取数据源
        JavaReceiverInputDStream<String> line = jssc.socketTextStream("hadoop104", 9999);
        JavaDStream<String> words = line.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordcount = pairs.reduceByKey((i1, i2) -> i1 + i2);
        wordcount.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}
