package com.adrien.rdd

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object CustomReceiver {
  def main(args: Array[String]): Unit = {
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))
    // 调用 receiverStream api，将自定义的Receiver传进去
    val lines = ssc.receiverStream(new CustomerReceivere("hadoop104", 9999))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

    ssc.stop(false)
  }
}
