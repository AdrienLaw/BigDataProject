package com.adrien.core.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JavaWordCountOnSpark {
    public static void main(String[] args) {
        //1、创建SparkConf对象
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");
        //2、构建JavaSparkContext对象
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //3、读取数据文件
        JavaRDD<String> data = sparkContext.textFile("/Users/luohaotian/Downloads/Jennifer/HelloApp/input/wordCount/wc.txt");
        //4、切分每一行获取所有的单词   scala:  data.flatMap(x=>x.split(" "))
        JavaRDD<String> stringJavaRDD = data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                String[] split = line.split(" ");
                return Arrays.asList(split).iterator();
            }
        });
        //5、每个单词计为1    scala:  wordsJavaRDD.map(x=>(x,1))
        JavaPairRDD<String, Integer> wordAndOne = stringJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String world) throws Exception {
                return new Tuple2<>(world, 1);
            }
        });
        //6、相同单词出现的1累加    scala:  wordAndOne.reduceByKey((x,y)=>x+y)
        JavaPairRDD<String, Integer> results = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        //按照单词出现的次数降序 (单词，次数)  -->(次数,单词).sortByKey----> (单词，次数)
        JavaPairRDD<Integer, String> reverseJavaRDD = results.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2, t._1);
            }
        });

        JavaPairRDD<String, Integer> sortedRDD = reverseJavaRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> is2) throws Exception {
                return new Tuple2<String, Integer>(is2._2, is2._1);
            }
        });
        //7、收集打印
        List<Tuple2<String, Integer>> tuple2List = sortedRDD.collect();
        for (Tuple2<String, Integer> stringIntegerTuple2 : tuple2List) {
            System.out.println("单词："+stringIntegerTuple2._1 +"\t次数："+stringIntegerTuple2._2);
        }
        sparkContext.stop();
    }
}
