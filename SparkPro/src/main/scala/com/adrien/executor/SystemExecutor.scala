package com.adrien.executor

import org.apache.spark.{SparkConf, SparkContext}

object SystemExecutor {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf() .setAppName("SystemExecutor")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd01 = sc.makeRDD(List(2,4,6,8))
    var sum = sc.longAccumulator("sum")
    rdd01.foreach(
      num => {
        sum.add(num)
      }
    )
    // 获取累加器的值
    println("sum = " + sum.value)

    new WordCountAccumulator


  }
}
