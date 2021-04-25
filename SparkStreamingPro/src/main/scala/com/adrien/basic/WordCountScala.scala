package com.adrien.basic

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    //初始化
    val conf = new SparkConf().setAppName("WordCountScala").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(1))
    //步骤二：获取数据流
    val lines = ssc.socketTextStream("hadoop104",9999)
    //步骤三：数据处理
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(words => (words,1))
    val wordCounts = pairs.reduceByKey(_+_)
    //步骤四： 数据输出
    wordCounts.print()
    //结束任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
