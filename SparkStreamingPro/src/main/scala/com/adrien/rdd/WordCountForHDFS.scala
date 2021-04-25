package com.adrien.rdd

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountForHDFS {
  def main(args: Array[String]): Unit = {
    //步骤一：初始化程序入口
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountForHDFS")
    val ssc = new StreamingContext(conf, Seconds(10))
    //步骤二：获取数据流
    val textFS: DStream[String] = ssc.textFileStream("hdfs://hadoop101:9000/input")
    //val lines = ssc.textFileStream("/input");
    //步骤三：数据处理
    val result: DStream[(String, Int)] = textFS.flatMap(_.split(" ").map((_,1))).reduceByKey(_+_)
    //步骤四： 数据输出
    result.print()
    //步骤五：启动任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
