package com.adrien.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RadioVal {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf() .setAppName("LineageRDD")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List( ("a",1), ("b", 2), ("c", 3), ("d", 4) ),4)
    val list = List( ("a",4), ("b", 5), ("c", 6), ("d", 7) )
    val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)
    val resultRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (key, num) => {
        var num2 = 0
        // 使用广播变量
        for ((k, v) <- broadcast.value) {
          if (k == key) { num2 = v
          } }
        (key, (num, num2))
      }
    }
  }
}
